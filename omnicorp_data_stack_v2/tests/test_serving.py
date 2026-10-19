import os
from pathlib import Path
import secrets
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pyarrow as pa
from utils.provision_secrets import provision
from utils.serving import postgres_type


class ServingContractTests(unittest.TestCase):
    def test_source_approved_secret_provisioning_never_overwrites(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / ".secrets"
            provision(root)
            before = {path.name: path.read_bytes() for path in root.iterdir()}
            provision(root)
            self.assertEqual(before, {path.name: path.read_bytes() for path in root.iterdir()})
            self.assertEqual(set(before), {"prefect_auth", "bi_postgres_admin", "bi_writer", "bi_reader", "metabase_app", "metabase_encryption", "metabase_session"})

    def test_arrow_types_map_without_losing_dates_or_ids(self):
        for actual, expected in [(pa.string(), "TEXT"), (pa.null(), "TEXT"), (pa.int64(), "BIGINT"),
                                 (pa.float64(), "DOUBLE PRECISION"), (pa.date32(), "DATE")]:
            self.assertEqual(postgres_type(actual), expected)
        with self.assertRaises(ValueError):
            postgres_type(pa.list_(pa.string()))


@unittest.skipUnless(os.getenv("RUN_POSTGRES_INTEGRATION") == "1", "requires an isolated PostgreSQL test server")
class PostgresServingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from utils.serving import initialize
        cls.secrets = tempfile.TemporaryDirectory()
        root = Path(cls.secrets.name)
        for name in ("bi_postgres_admin", "bi_writer", "bi_reader", "metabase_app"):
            value = os.environ["POSTGRES_TEST_PASSWORD"] if name == "bi_postgres_admin" else secrets.token_urlsafe(32)
            (root / name).write_text(value)
        cls.environment = patch.dict(os.environ, {"BI_SECRETS_DIR": str(root), "SERVING_ENABLED": "false"})
        cls.environment.start()
        initialize()

    @classmethod
    def tearDownClass(cls):
        cls.environment.stop()
        cls.secrets.cleanup()

    def publish_fixture(self, root):
        from flow import flow_main as pipeline
        from utils import runtime
        from test_pipeline import source_tables, offline_flow
        with patch.object(pipeline, "build_chinook_landing", return_value=source_tables("chinook")), patch.object(
            pipeline, "build_northwind_landing", return_value=source_tables("northwind")
        ), patch.object(pipeline, "main_flow", side_effect=offline_flow):
            return runtime.execute(root)

    def test_publish_is_idempotent_and_reader_cannot_write(self):
        import psycopg
        from utils.serving import connection_options, publish
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run_id = self.publish_fixture(root)
            publish(root, run_id)
            with psycopg.connect(**connection_options()) as connection:
                row = connection.execute("SELECT run_id, loaded_at FROM integration._pipeline_run").fetchone()
                self.assertEqual(row[0], run_id)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM integration.fact_order").fetchone()[0], 2)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM pg_constraint c JOIN pg_namespace n ON n.oid=c.connamespace WHERE n.nspname='integration' AND c.contype='f'").fetchone()[0], 7)
            publish(root, run_id)
            with psycopg.connect(**connection_options()) as connection:
                self.assertEqual(connection.execute("SELECT loaded_at FROM integration._pipeline_run").fetchone()[0], row[1])
            with psycopg.connect(**connection_options("metabase_reader", "omnicorp", "bi_reader")) as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM integration.fact_order").fetchone()[0], 2)
                with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                    connection.execute("DELETE FROM integration.fact_order")
                connection.rollback()

    def test_copy_failure_rolls_back_all_tables_and_metadata(self):
        import psycopg
        from utils.serving import connection_options, publish
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = self.publish_fixture(root)
            publish(root, first)
            second = self.publish_fixture(root)
            with patch("utils.serving._copy_table", side_effect=RuntimeError("simulated COPY failure")):
                with self.assertRaisesRegex(RuntimeError, "COPY failure"):
                    publish(root, second)
            with psycopg.connect(**connection_options()) as connection:
                self.assertEqual(connection.execute("SELECT run_id FROM integration._pipeline_run").fetchone()[0], first)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM integration.fact_order").fetchone()[0], 2)
                self.assertEqual(connection.execute("SELECT SUM(amount) FROM integration.fact_order").fetchone()[0], 30)
