import sys
from pathlib import Path
import sqlite3
from contextlib import closing
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pandas as pd
from raw_fixtures import raw_inputs
from utils.eda import load_raw_inputs, landing_preview, preview_layers
from utils.modeling import build_source_models
from utils.config import INGRESS_TABLES


class LayerContractTests(unittest.TestCase):
    def test_raw_eda_works_before_pipeline_with_local_sources(self):
        raw = raw_inputs()
        with tempfile.TemporaryDirectory() as directory:
            paths = {}
            for source in INGRESS_TABLES:
                paths[source] = Path(directory) / f"{source}.db"
                with closing(sqlite3.connect(paths[source])) as connection:
                    for key, name in INGRESS_TABLES[source].items():
                        raw[f"{source}.{key}"].to_sql(name, connection, index=False)
            with patch("utils.eda.download_source", side_effect=AssertionError("unexpected network")):
                actual = load_raw_inputs("dim_customer", db_paths=paths)
            self.assertEqual(actual["chinook.customer"].city.iloc[0], " Bangkok ")
            self.assertEqual(actual["chinook.customer"].customerid.iloc[0], 1)

    def test_landing_uses_raw_values_and_never_runs_monadsquishy(self):
        raw = raw_inputs()
        raw["chinook.employee"]["reportsto"] = "invalid"
        def reader(source, key):
            return raw[f"{source}.{key}"].copy()
        with patch("monadsquishy.monadsquishy.SquishyEngine.run_batch", side_effect=AssertionError("cleaning in Landing")):
            tables = build_source_models("chinook", reader)
            preview = landing_preview("fact_order", raw)
            self.assertEqual(preview["chinook"]["dim_date"].date_id.iloc[0], "01/02/2024")
        self.assertEqual(tables["dim_customer"].country.iloc[0], " Thailand ")
        self.assertEqual(tables["dim_employee"].reportsto.iloc[0], "invalid")
        self.assertEqual(tables["fact_order"].date_id.iloc[0], "01/02/2024")
        self.assertTrue(tables["dim_date"].calendar_year.isna().all())
        self.assertEqual(tables["fact_order"].employee_id.iloc[0], tables["dim_employee"].employee_id.iloc[0])

    def test_notebook_preview_matches_staging_rules_and_consolidates(self):
        preview = preview_layers("fact_order", raw_inputs())
        self.assertEqual(preview["landing"]["chinook"].date_id.iloc[0], "01/02/2024")
        self.assertEqual(str(preview["staging"]["chinook"].date_id.iloc[0]), "2024-02-01")
        self.assertEqual(len(preview["integration"]), 2)
        self.assertEqual(preview["integration"].amount.sum(), 35)
        self.assertTrue(preview["integration"].quality_status.eq("passed").all())

    def test_missing_dimension_is_not_invented_at_landing(self):
        raw = raw_inputs()
        raw["chinook.employee"]["employeeid"] = 99
        preview = preview_layers("fact_order", raw)
        fact = preview["landing"]["chinook"]
        self.assertEqual(len(fact), 1)
        self.assertTrue(fact.employee_id.isna().all())
        self.assertEqual(fact.source_employee_id.iloc[0], 1)
        self.assertTrue(preview["staging"]["chinook"].quality_issues.str.contains("missing_dim_employee").all())

    def test_equivalent_raw_dates_conform_only_in_staging(self):
        from flow.flow_dim_date import build_landing, stage_dataframe
        landing = build_landing(pd.DataFrame({"order_date": ["01/02/2024", "2024-02-01", "invalid"]}))
        self.assertEqual(len(landing), 3)
        staging, metadata = stage_dataframe(landing)
        self.assertEqual(len(staging), 2)
        self.assertEqual(staging.date_id.notna().sum(), 1)
        self.assertTrue(any(item["status"] == "failed" for item in metadata))

    def test_date_key_must_describe_the_actual_order_date(self):
        from utils.schema import MODEL_TABLES
        from utils.quality import annotate_fact_quality, validate_model_integrity
        raw = raw_inputs()
        tables = {table: preview_layers(table, raw)["integration"] for table in MODEL_TABLES}
        tables["fact_order"].loc[0, "date_id"] = tables["dim_date"].date_id.iloc[1]
        self.assertIn("date_mismatch", annotate_fact_quality(tables).quality_issues.iloc[0])
        self.assertTrue(any("mismatches with order_date" in item for item in validate_model_integrity(tables)[1]))
