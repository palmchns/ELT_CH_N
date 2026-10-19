"""Opt-in integration tests against an isolated, authenticated Prefect API."""
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from flow import flow_main as pipeline
from utils import runtime
from test_pipeline import source_tables


@unittest.skipUnless(os.getenv("RUN_PREFECT_INTEGRATION") == "1", "requires an isolated Prefect API")
class PrefectRuntimeTests(unittest.TestCase):
    def test_flow_publishes_through_authenticated_api(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(
            pipeline, "task1_ingress", return_value=None
        ), patch.object(pipeline, "build_chinook_landing", return_value=source_tables("chinook")), patch.object(
            pipeline, "build_northwind_landing", return_value=source_tables("northwind")
        ), patch.dict(os.environ, {"MAX_FAILED_RULE_CHECKS": "0", "SERVING_ENABLED": "false"}):
            root = Path(directory)
            run_id = runtime.execute(root)
            self.assertEqual(runtime.verify_snapshot(root, run_id)["artifacts"]["fact_order"]["row_count"], 2)

    def test_flow_quality_failure_does_not_publish(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(
            pipeline, "task1_ingress", return_value=None
        ), patch.object(pipeline, "build_chinook_landing", return_value=source_tables("chinook", bad=True)), patch.object(
            pipeline, "build_northwind_landing", return_value=source_tables("northwind")
        ), patch.dict(os.environ, {"MAX_FAILED_RULE_CHECKS": "0", "SERVING_ENABLED": "false"}):
            root = Path(directory)
            with self.assertRaisesRegex(ValueError, "Model integrity failed"):
                runtime.execute(root)
            self.assertFalse((root / "latest.json").exists())
