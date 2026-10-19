"""Exercise launcher paths, failure handling and persistent-data boundaries without Docker."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


class LauncherTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.project = self.root / "shared project" / "omnicorp_data_stack_v2"
        self.project.mkdir(parents=True)
        source = Path(__file__).resolve().parents[1]
        for name in ("ELT_V2.sh", ".env.example", "docker-compose.prod.yml", "utils/provision_secrets.py"):
            destination = self.project / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source / name, destination)
        binaries = self.root / "bin"
        binaries.mkdir()
        (binaries / "python3").symlink_to(sys.executable)
        docker = binaries / "docker"
        docker.write_text(f"#!{sys.executable}\n" + '''import json, os, sys
with open(os.environ["DOCKER_CALLS"], "a") as output:
    output.write(json.dumps({"args": sys.argv[1:], "cwd": os.getcwd()}) + "\\n")
if os.environ.get("DOCKER_FAIL_TOKEN") in sys.argv[1:]:
    sys.exit(23)
''')
        docker.chmod(0o755)
        self.log = self.root / "docker.jsonl"
        self.environment = dict(os.environ, PATH=str(binaries) + os.pathsep + os.environ["PATH"],
                                DOCKER_CALLS=str(self.log), DOCKER_FAIL_TOKEN="")

    def launch(self, *args):
        return subprocess.run(["bash", str(self.project / "ELT_V2.sh"), *args], cwd=self.root,
                              env=self.environment, capture_output=True, text=True)

    def calls(self):
        return [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []

    def test_start_works_without_sibling_folders_and_preserves_credentials(self):
        (self.project / ".env").write_text("METABASE_PORT=3099\n")
        result = self.launch()
        self.assertEqual(result.returncode, 0, result.stderr)
        before = {file.name: file.read_bytes() for file in (self.project / ".secrets").iterdir()}
        self.assertEqual(len(before), 7)
        self.assertEqual(self.launch("start").returncode, 0)
        self.assertEqual(before, {file.name: file.read_bytes() for file in (self.project / ".secrets").iterdir()})
        self.assertEqual((self.project / ".env").read_text(), "METABASE_PORT=3099\n")
        calls = self.calls()
        self.assertTrue(all(call["cwd"] == str(self.project.resolve()) for call in calls))
        arguments = [call["args"] for call in calls]
        initialization = next(i for i, args in enumerate(arguments) if args[-1] == "bi_init")
        dashboard = next(i for i, args in enumerate(arguments) if args[-1] == "metabase")
        publication = next(i for i, args in enumerate(arguments) if args[-3:] == ["python", "run_pipeline.py", "run"])
        self.assertLess(initialization, dashboard)
        self.assertLess(dashboard, publication)
        for args in arguments:
            if "--project-directory" in args:
                self.assertEqual(Path(args[args.index("--project-directory") + 1]).resolve(), self.project.resolve())

    def test_initialization_failure_stops_before_metabase_and_publication(self):
        self.environment["DOCKER_FAIL_TOKEN"] = "bi_init"
        self.assertEqual(self.launch("start").returncode, 23)
        arguments = [call["args"] for call in self.calls()]
        self.assertFalse(any(args[-1] == "metabase" or "run_pipeline.py" in args for args in arguments))

    def test_docker_unavailable_does_not_create_configuration(self):
        self.environment["DOCKER_FAIL_TOKEN"] = "info"
        result = self.launch("start")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Start Docker", result.stderr)
        self.assertFalse((self.project / ".env").exists())
        self.assertFalse((self.project / ".secrets").exists())

    def test_status_forwards_arguments_and_stop_preserves_volumes(self):
        self.assertEqual(self.launch("status", "--max-age-hours", "48").returncode, 0)
        self.assertEqual(self.calls()[-1]["args"][-5:], ["python", "run_pipeline.py", "status", "--max-age-hours", "48"])
        self.assertEqual(self.launch("stop").returncode, 0)
        self.assertEqual(self.calls()[-1]["args"][-3:], ["--profile", "jobs", "down"])
        self.assertNotIn("--volumes", self.calls()[-1]["args"])

    def test_help_invalid_arguments_and_check_have_no_setup_side_effects(self):
        self.assertEqual(self.launch("help").returncode, 0)
        self.assertEqual(self.launch("unknown").returncode, 2)
        self.assertEqual(self.launch("start", "unexpected").returncode, 2)
        self.assertEqual(self.calls(), [])
        self.environment["DOCKER_FAIL_TOKEN"] = "info"
        self.assertEqual(self.launch("check").returncode, 0)
        self.assertFalse(any(call["args"] == ["info"] for call in self.calls()))
        self.assertFalse((self.project / ".env").exists())
        self.assertFalse((self.project / ".secrets").exists())
