import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pandas as pd
import pyarrow.parquet as pq

from flow import flow_main as pipeline
from monadsquishy.monadsquishy import SquishyEngine
from monadsquishy.rules import flexible_date_parser
from utils import runtime
from container_entrypoint import environment
from utils.eda import load_published_table
from raw_fixtures import raw_inputs


def source_tables(source, bad=False):
    return {
        "dim_customer": pd.DataFrame([dict(source_system=source, customer_id=f"{source}:1", customerid="1",
                                            city="Bangkok", country="Thailand", email=" user@example.com ")]),
        "dim_employee": pd.DataFrame([dict(source_system=source, employee_id=f"{source}:1", employeeid=1,
                                            manager_id=None, city="Bangkok", country="Thailand")]),
        "dim_product": pd.DataFrame([dict(source_system=source, product_id=f"{source}:1", unit_price=10)]),
        "fact_order": pd.DataFrame([dict(source_system=source, order_id=f"{source}:1", order_line_id=f"{source}:1",
                                          customer_id=f"{source}:1", employee_id=f"{source}:1", product_id=f"{source}:1",
                                          source_customer_id="1", source_employee_id="1", source_product_id="1",
                                          order_date="01/02/2024", quantity=-2 if bad else 2, unit_price=10,
                                          discount=0.25, amount=-15 if bad else 15)]),
    }


def offline_flow():
    pipeline.task2_landing.fn()
    with patch.dict(pipeline.MODEL_FLOWS, {table: task.fn for table, task in pipeline.MODEL_FLOWS.items()}):
        pipeline.task3_staging.fn()
    pipeline.check_staging_quality.fn()
    pipeline.task4_integration.fn()


class RuleTests(unittest.TestCase):
    def clean(self, column, values, table="fact_order"):
        df = pd.DataFrame({column: values})
        engine = SquishyEngine(pipeline.staging_config(df, table), df)
        return engine.run_batch()[column], engine.logs

    def test_numeric_constraints_null_invalid_values(self):
        for column, values, expected_null in [
            ("quantity", [-1, 0, 2, None], [True, True, False, True]),
            ("unit_price", [-1, 0, 2, None], [True, False, False, True]),
            ("amount", [-1, 0, 2, None], [True, False, False, True]),
            ("discount", [-0.1, 0, 1, 1.1], [True, False, False, True]),
        ]:
            with self.subTest(column=column):
                result, logs = self.clean(column, values)
                self.assertEqual(result.isna().tolist(), expected_null)
                self.assertGreater(logs.loc[logs.status.eq("failed"), "count"].sum(), 0)

    def test_email_uses_normalized_value(self):
        result, _ = self.clean("email", [" user@example.com ", "broken", None], "dim_customer")
        self.assertEqual(result.iloc[0], "user@example.com")
        self.assertTrue(result.iloc[1:].isna().all())

    def test_dates_match_scalar_rule(self):
        values = ["01/02/2024", "2567-01-01", "2024-02-29", "not-a-date", None]
        result, _ = self.clean("order_date", values)
        self.assertEqual([str(value) for value in result[:3]], [flexible_date_parser(value) for value in values[:3]])
        self.assertTrue(result.iloc[3:].isna().all())

    def test_date_dimension_matches_fact_parser(self):
        tables = pipeline.add_conformed_dimensions(source_tables("chinook"))
        self.assertEqual(tables["dim_date"].date_id.iloc[0], "01/02/2024")
        cleaned, _ = pipeline.MODELS["dim_date"].stage_dataframe(tables["dim_date"])
        self.assertEqual(str(cleaned.date_id.iloc[0]), "2024-02-01")

    def test_empty_batch_preserves_columns(self):
        result, _ = self.clean("order_date", [])
        self.assertTrue(result.empty)


class IngestionAndSecretTests(unittest.TestCase):
    def test_download_rejects_invalid_payload_and_cleans_temporary_file(self):
        response = MagicMock()
        response.__enter__.return_value = response
        response.iter_content.return_value = [b"this is not SQLite"]
        with tempfile.TemporaryDirectory() as directory:
            original = tempfile.NamedTemporaryFile
            with patch.object(pipeline.requests, "get", return_value=response), patch.object(
                pipeline.tempfile, "NamedTemporaryFile", side_effect=lambda **kwargs: original(dir=directory, **kwargs)
            ):
                with self.assertRaisesRegex(ValueError, "not a SQLite"):
                    pipeline.download_source("chinook", "https://example.invalid/db")
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_download_enforces_size_and_checksum(self):
        for variables, message in [({"MAX_SOURCE_BYTES": "4"}, "exceeds"),
                                   ({"CHINOOK_SHA256": "0" * 64}, "checksum mismatch")]:
            response = MagicMock()
            response.__enter__.return_value = response
            response.iter_content.return_value = [b"SQLite format 3\x00"]
            with self.subTest(variables=variables), patch.object(pipeline.requests, "get", return_value=response), patch.dict(
                os.environ, variables
            ):
                with self.assertRaisesRegex(ValueError, message):
                    pipeline.download_source("chinook", "https://example.invalid/db")

    def test_secret_is_loaded_and_cloud_key_removed(self):
        with tempfile.TemporaryDirectory() as directory:
            secret = Path(directory) / "credential"
            secret.write_text("operator:strong-test-password\n")
            with patch.dict(os.environ, {"REQUIRE_API_AUTH": "true", "PREFECT_AUTH_FILE": str(secret), "PREFECT_API_KEY": "cloud"}):
                result = environment()
            self.assertEqual(result["PREFECT_API_AUTH_STRING"], "operator:strong-test-password")
            self.assertEqual(result["PREFECT_SERVER_API_AUTH_STRING"], result["PREFECT_API_AUTH_STRING"])
            self.assertNotIn("PREFECT_API_KEY", result)

    def test_missing_or_malformed_secret_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            secret = Path(directory) / "credential"
            with patch.dict(os.environ, {"REQUIRE_API_AUTH": "true", "PREFECT_AUTH_FILE": str(secret)}):
                with self.assertRaises(FileNotFoundError):
                    environment()
                secret.write_text("invalid-secret-value")
                with self.assertRaisesRegex(ValueError, "username:password") as failure:
                    environment()
                self.assertNotIn("invalid-secret-value", str(failure.exception))


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.bad = False
        self.patches = [
            patch.object(pipeline, "build_chinook_landing", side_effect=lambda: source_tables("chinook", self.bad)),
            patch.object(pipeline, "build_northwind_landing", side_effect=lambda: source_tables("northwind", self.bad)),
            patch.object(pipeline, "main_flow", side_effect=offline_flow),
            patch.dict("os.environ", {"MAX_FAILED_RULE_CHECKS": "0", "SERVING_ENABLED": "false"}),
        ]
        for item in self.patches:
            item.start()

    def tearDown(self):
        for item in reversed(self.patches):
            item.stop()
        self.temporary.cleanup()

    def test_complete_run_publishes_valid_snapshot_and_metadata(self):
        run_id = runtime.execute(self.root)
        manifest = runtime.verify_snapshot(self.root, run_id)
        snapshot = runtime.run_path(self.root, run_id)
        self.assertEqual(set(manifest["artifacts"]), set(pipeline.MODEL_TABLES))
        self.assertEqual(manifest["published_zone"], "integration")
        self.assertTrue(all(item["path"].startswith("integration/") for item in manifest["artifacts"].values()))
        self.assertFalse((snapshot / "datamart").exists())
        fact = pq.ParquetFile(snapshot / manifest["artifacts"]["fact_order"]["path"]).read().to_pandas()
        self.assertEqual(len(fact), 2)
        self.assertTrue(fact.quality_status.eq("passed").all())
        self.assertEqual(fact.amount.sum(), 30)
        integration = next((snapshot / "integration" / "fact_order").rglob("*.parquet"))
        metadata = pq.read_metadata(integration).metadata
        self.assertIn(b"quality_context", metadata)
        self.assertIn(b"relationship_summary", metadata)
        runtime.status(self.root)
        self.assertFalse((self.root / "pipeline.prom").exists())

    def test_all_eda_notebooks_execute_without_changing_snapshot(self):
        project = Path(__file__).resolve().parents[1]
        raw = raw_inputs()
        before = {key: frame.copy(deep=True) for key, frame in raw.items()}
        notebooks = sorted((project / "notebook").glob("*.ipynb"))
        self.assertEqual({path.stem for path in notebooks}, set(pipeline.MODEL_TABLES))
        for path in notebooks:
            with self.subTest(notebook=path.name), patch("utils.eda.load_raw_inputs", return_value=raw), patch.object(
                Path, "cwd", return_value=project / "notebook"
            ):
                notebook = json.loads(path.read_text())
                namespace = {"display": lambda *args: None}
                for cell in notebook["cells"]:
                    if cell["cell_type"] == "code":
                        self.assertEqual(cell["outputs"], [])
                        exec(compile("".join(cell["source"]), str(path), "exec"), namespace)
                self.assertFalse(namespace["integration"].empty)
        self.assertFalse((self.root / "latest.json").exists())
        for key, frame in raw.items():
            pd.testing.assert_frame_equal(before[key], frame)

    def test_eda_rejects_incomplete_snapshots(self):
        snapshot = runtime.run_path(self.root, "pending")
        snapshot.mkdir(parents=True)
        runtime.write_json(snapshot / "manifest.json", {"schema_version": 1, "status": "running", "run_id": "pending"})
        with self.assertRaisesRegex(ValueError, "incomplete"):
            load_published_table("dim_customer", root=self.root, run_id="pending")

    def test_eda_reports_unknown_table_and_missing_publication(self):
        with self.assertRaisesRegex(ValueError, "Unknown model table"):
            load_published_table("unknown", root=self.root)
        with self.assertRaisesRegex(FileNotFoundError, "No published snapshot"):
            load_published_table("dim_customer", root=self.root)

    def test_failed_quality_never_replaces_previous_publication(self):
        run_id = runtime.execute(self.root)
        self.bad = True
        with self.assertRaisesRegex(ValueError, "Model integrity failed"):
            runtime.execute(self.root)
        self.assertEqual(json.loads((self.root / "latest.json").read_text())["run_id"], run_id)
        manifests = [json.loads(path.read_text()) for path in self.root.glob("runs/*/manifest.json")]
        self.assertEqual(sorted(item["status"] for item in manifests), ["complete", "failed"])

    def test_rule_tolerance_cannot_allow_missing_order_id(self):
        tables = source_tables("chinook")
        tables["fact_order"]["order_id"] = None
        with patch.object(pipeline, "build_chinook_landing", return_value=tables), patch.dict(
            os.environ, {"MAX_FAILED_RULE_CHECKS": "999"}
        ):
            with self.assertRaisesRegex(ValueError, "Model integrity failed"):
                runtime.execute(self.root)
        self.assertFalse((self.root / "latest.json").exists())

    def test_exception_keeps_previous_publication(self):
        run_id = runtime.execute(self.root)
        with patch.object(pipeline, "main_flow", side_effect=RuntimeError("failure")):
            with self.assertRaises(RuntimeError):
                runtime.execute(self.root)
        self.assertEqual(json.loads((self.root / "latest.json").read_text())["run_id"], run_id)

    def test_serving_failure_retains_complete_lake_snapshot_and_is_visible(self):
        with patch.dict(os.environ, {"SERVING_ENABLED": "true"}), patch("utils.serving.publish", side_effect=RuntimeError("database unavailable")):
            with self.assertRaisesRegex(RuntimeError, "database unavailable"):
                runtime.execute(self.root)
        pointer = json.loads((self.root / "latest.json").read_text())
        self.assertEqual(runtime.verify_snapshot(self.root, pointer["run_id"])["status"], "complete")
        with patch.dict(os.environ, {"SERVING_ENABLED": "true"}):
            with self.assertRaises(FileNotFoundError):
                runtime.status(self.root)

    def test_bi_is_never_called_for_invalid_data(self):
        self.bad = True
        with patch.dict(os.environ, {"SERVING_ENABLED": "true"}), patch("utils.serving.publish") as publish:
            with self.assertRaises(ValueError):
                runtime.execute(self.root)
            publish.assert_not_called()

    def test_health_requires_matching_serving_snapshot_when_enabled(self):
        run_id = runtime.execute(self.root)
        runtime.write_json(self.root / "serving.json", {"status": "complete", "run_id": "older"})
        with patch.dict(os.environ, {"SERVING_ENABLED": "true"}):
            with self.assertRaisesRegex(ValueError, "not synchronized"):
                runtime.status(self.root)
            runtime.write_json(self.root / "serving.json", {"status": "complete", "run_id": run_id})
            runtime.status(self.root)

    def test_rollback_checks_artifacts_and_records_audit(self):
        first = runtime.execute(self.root)
        second = runtime.execute(self.root)
        self.assertNotEqual(first, second)
        runtime.rollback(self.root, first)
        self.assertEqual(json.loads((self.root / "latest.json").read_text())["run_id"], first)
        self.assertEqual(len(list((self.root / "recovery").glob("*.json"))), 1)
        manifest = runtime.verify_snapshot(self.root, second)
        artifact = self.root / "runs" / second / manifest["artifacts"]["fact_order"]["path"]
        artifact.write_bytes(b"corrupt")
        with self.assertRaisesRegex(ValueError, "mismatch"):
            runtime.rollback(self.root, second)
        self.assertEqual(json.loads((self.root / "latest.json").read_text())["run_id"], first)

    def test_lock_rejects_concurrent_writer(self):
        with runtime.writer_lock(self.root):
            with self.assertRaisesRegex(RuntimeError, "writer lock"):
                runtime.execute(self.root)

    def test_atomic_file_failure_preserves_existing_parquet(self):
        path = self.root / "test.parquet"
        pipeline.write_parquet(pd.DataFrame({"value": [1]}), path)
        before = path.read_bytes()
        with patch.object(pipeline.pq, "write_table", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                pipeline.write_parquet(pd.DataFrame({"value": [2]}), path)
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(list(self.root.glob(".parquet-*")), [])

    def test_unsafe_run_id_is_rejected(self):
        with self.assertRaises(ValueError):
            runtime.rollback(self.root, "../outside")

    def test_unknown_manifest_version_is_rejected(self):
        snapshot = runtime.run_path(self.root, "future")
        snapshot.mkdir(parents=True)
        runtime.write_json(snapshot / "manifest.json", {"schema_version": 999, "status": "complete", "run_id": "future"})
        with self.assertRaisesRegex(ValueError, "Unsupported manifest"):
            runtime.verify_snapshot(self.root, "future")

    def test_backup_restore_recovers_identical_publication(self):
        run_id = runtime.execute(self.root)
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "backup"
            restored = Path(directory) / "restored"
            runtime.backup(self.root, destination)
            runtime.restore(restored, destination)
            self.assertEqual(runtime.verify_snapshot(self.root, run_id), runtime.verify_snapshot(restored, run_id))
            runtime.status(restored)
            with self.assertRaises(FileExistsError):
                runtime.backup(self.root, destination)

    def test_corrupt_backup_cannot_replace_publication(self):
        run_id = runtime.execute(self.root)
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "backup"
            runtime.backup(self.root, destination)
            manifest = runtime.verify_snapshot(destination, run_id)
            artifact = runtime.run_path(destination, run_id) / manifest["artifacts"]["fact_order"]["path"]
            artifact.write_bytes(b"corrupt")
            with self.assertRaisesRegex(ValueError, "mismatch"):
                runtime.restore(self.root, destination)
            self.assertEqual(json.loads((self.root / "latest.json").read_text())["run_id"], run_id)

    def test_stale_snapshot_fails_health_check(self):
        runtime.execute(self.root)
        with self.assertRaisesRegex(ValueError, "stale"):
            runtime.status(self.root, max_age_hours=0)


if __name__ == "__main__":
    unittest.main()
