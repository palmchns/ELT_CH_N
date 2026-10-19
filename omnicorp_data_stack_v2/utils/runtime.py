"""Single-writer snapshot publication, status checks, and recovery for the local lake."""

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
from importlib.metadata import version
import json
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile
import time
import uuid

import pyarrow.parquet as pq


def atomic_write(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".pending-", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def write_json(path, value):
    atomic_write(path, json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def event(name, **fields):
    print(json.dumps({"event": name, "timestamp": datetime.now(timezone.utc).isoformat(), **fields}), flush=True)


@contextmanager
def writer_lock(root):
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".writer.lock").open("a") as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError("Another run or rollback holds the writer lock") from error
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def run_path(root, run_id):
    if not re.fullmatch(r"[A-Za-z0-9_-]+", run_id):
        raise ValueError("Invalid run ID")
    return root / "runs" / run_id


def checksum(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def sync_tree(root):
    """Flush copied files and directory entries before publishing a restored tree."""
    for directory, _, files in os.walk(root, topdown=False):
        for name in files:
            with (Path(directory) / name).open("rb") as stream:
                os.fsync(stream.fileno())
        sync_directory(directory)


def verify_snapshot(root, run_id, hashes=True):
    snapshot = run_path(root, run_id)
    manifest = json.loads((snapshot / "manifest.json").read_text())
    if manifest.get("schema_version") not in {1, 2}:
        raise ValueError("Unsupported manifest schema version")
    if manifest.get("status") != "complete" or manifest.get("run_id") != run_id:
        raise ValueError("Snapshot is incomplete")
    from utils.schema import MODEL_TABLES
    artifacts = manifest.get("artifacts", {})
    if set(artifacts) != set(MODEL_TABLES):
        raise ValueError("Snapshot does not contain every model table")
    for artifact in artifacts.values():
        path = (snapshot / artifact["path"]).resolve()
        if not path.is_relative_to(snapshot.resolve()) or not path.is_file():
            raise ValueError("Artifact is missing or outside snapshot")
        if path.stat().st_size != artifact["size_bytes"] or (hashes and checksum(path) != artifact["sha256"]):
            raise ValueError("Artifact checksum or size mismatch")
    return manifest


def quality_gate(snapshot, max_failed_checks=0):
    """Never publish broken fact relationships; optionally tolerate rule failures."""
    failures = 0
    files = list((snapshot / "staging").glob("*/year=*/month=*/day=*/*.parquet"))
    from utils.schema import MODEL_TABLES
    from utils.config import SOURCES
    if len(files) != len(MODEL_TABLES) * len(SOURCES):
        raise ValueError("Staging snapshot is incomplete")
    for path in files:
        metadata = pq.read_metadata(path).metadata or {}
        context = json.loads(metadata[b"quality_context"])
        relationships = json.loads(metadata[b"relationship_summary"])
        failures += context["failed_rule_checks"]
        if context.get("flagged_rows") or context.get("amount_mismatch_count") or any(
            relationship["status"] == "failed" for relationship in relationships
        ):
            raise ValueError(f"Model integrity failed: {path.name}")
    if failures > max_failed_checks:
        raise ValueError(f"Quality gate failed: {failures} rule failures exceed {max_failed_checks}")
    return failures


def execute(root):
    from flow import flow_main as pipeline
    with writer_lock(root):
        now = datetime.now(timezone.utc)
        run_id = now.strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:12]
        snapshot = run_path(root, run_id)
        snapshot.mkdir(parents=True)
        manifest = {
            "schema_version": 2, "run_id": run_id, "status": "running", "started_at": now.isoformat(),
            "published_zone": "integration",
            "runtime_versions": {name: version(name) for name in ("prefect", "pandas", "duckdb", "pyarrow", "requests", "psycopg")},
        }
        write_json(snapshot / "manifest.json", manifest)
        event("run_started", run_id=run_id)
        old_values = (pipeline.OBS_DIR, pipeline.TODAY, pipeline.YEAR, pipeline.MONTH, pipeline.DAY)
        try:
            pipeline.OBS_DIR = str(snapshot)
            pipeline.TODAY = now
            pipeline.YEAR, pipeline.MONTH, pipeline.DAY = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")
            pipeline.main_flow()
            threshold = int(os.getenv("MAX_FAILED_RULE_CHECKS", "0"))
            if threshold < 0:
                raise ValueError("MAX_FAILED_RULE_CHECKS must be non-negative")
            failures = quality_gate(snapshot, threshold)
            artifacts = {}
            for table in pipeline.MODEL_TABLES:
                path = Path(pipeline.integration_path(table))
                artifacts[table] = {"path": str(path.relative_to(snapshot)), "size_bytes": path.stat().st_size,
                                    "sha256": checksum(path), "row_count": pq.read_metadata(path).num_rows}
            manifest.update(status="complete", completed_at=datetime.now(timezone.utc).isoformat(),
                            failed_rule_checks=failures, max_failed_rule_checks=threshold, artifacts=artifacts)
            sync_tree(snapshot)
            sync_directory(snapshot.parent)
            write_json(snapshot / "manifest.json", manifest)
            verify_snapshot(root, run_id)
            # Readers resolve this pointer once and use only that immutable snapshot.
            write_json(root / "latest.json", {"run_id": run_id})
            if os.getenv("SERVING_ENABLED", "false").lower() == "true":
                from utils.serving import publish
                publish(root, run_id)
        except BaseException as error:
            if manifest.get("status") != "complete":
                manifest.update(status="failed", error_type=type(error).__name__,
                                completed_at=datetime.now(timezone.utc).isoformat())
                write_json(snapshot / "manifest.json", manifest)
            event("run_failed", run_id=run_id, error_type=type(error).__name__)
            raise
        finally:
            pipeline.OBS_DIR, pipeline.TODAY, pipeline.YEAR, pipeline.MONTH, pipeline.DAY = old_values
        event("run_published", run_id=run_id, failed_rule_checks=failures)
        return run_id


def status(root, max_age_hours=26):
    pointer = json.loads((root / "latest.json").read_text())
    manifest = verify_snapshot(root, pointer["run_id"], hashes=False)
    age = (datetime.now(timezone.utc) - datetime.fromisoformat(manifest["completed_at"])).total_seconds()
    if not 0 <= age <= max_age_hours * 3600:
        raise ValueError("Published snapshot is stale or dated in the future")
    if os.getenv("SERVING_ENABLED", "false").lower() == "true":
        serving = json.loads((root / "serving.json").read_text())
        if serving.get("status") != "complete" or serving.get("run_id") != manifest["run_id"]:
            raise ValueError("Metabase serving is not synchronized with the published Integration snapshot")
    event("snapshot_healthy", run_id=manifest["run_id"], age_seconds=int(age))


def rollback(root, run_id):
    with writer_lock(root):
        verify_snapshot(root, run_id)
        previous = json.loads((root / "latest.json").read_text()) if (root / "latest.json").exists() else None
        record = {"run_id": run_id, "previous": previous, "at": datetime.now(timezone.utc).isoformat()}
        write_json(root / "recovery" / f"{time.time_ns()}.json", record)
        write_json(root / "latest.json", {"run_id": run_id})
        event("snapshot_rolled_back", **record)


def backup(root, destination):
    """Copy one immutable published snapshot into a new, independently verifiable root."""
    pointer = json.loads((root / "latest.json").read_text())
    run_id = pointer["run_id"]
    verify_snapshot(root, run_id)
    source = run_path(root, run_id)
    if destination.resolve().is_relative_to(source.resolve()):
        raise ValueError("Backup destination must be outside the source snapshot")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".backup-", dir=destination.parent))
    try:
        shutil.copytree(source, run_path(temporary, run_id))
        verify_snapshot(temporary, run_id)
        write_json(temporary / "latest.json", pointer)
        sync_tree(temporary)
        # Refuse to merge into an existing backup.
        if destination.exists():
            raise FileExistsError("Backup destination already exists")
        os.rename(temporary, destination)
        sync_directory(destination.parent)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)
    event("snapshot_backed_up", run_id=run_id)


def restore(root, backup_root):
    pointer = json.loads((backup_root / "latest.json").read_text())
    run_id = pointer["run_id"]
    verify_snapshot(backup_root, run_id)
    with writer_lock(root):
        destination = run_path(root, run_id)
        if destination.exists():
            verify_snapshot(root, run_id)
        else:
            destination.parent.mkdir(parents=True, exist_ok=True)
            temporary = Path(tempfile.mkdtemp(prefix=".restore-", dir=root))
            try:
                shutil.copytree(run_path(backup_root, run_id), run_path(temporary, run_id))
                verify_snapshot(temporary, run_id)
                sync_tree(temporary)
                os.rename(run_path(temporary, run_id), destination)
                sync_directory(destination.parent)
            finally:
                shutil.rmtree(temporary)
        previous = json.loads((root / "latest.json").read_text()) if (root / "latest.json").exists() else None
        write_json(root / "recovery" / f"{time.time_ns()}.json", {
            "action": "restore", "run_id": run_id, "previous": previous,
            "at": datetime.now(timezone.utc).isoformat(),
        })
        write_json(root / "latest.json", pointer)
    event("snapshot_restored", run_id=run_id)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["run", "status", "rollback", "backup", "restore"])
    parser.add_argument("--root", type=Path, default=Path(os.getenv("OBS_DIR", "./obs_data")))
    parser.add_argument("--run-id")
    parser.add_argument("--max-age-hours", type=float, default=26)
    parser.add_argument("--destination", type=Path)
    parser.add_argument("--backup", type=Path)
    args = parser.parse_args()
    if args.command == "run":
        execute(args.root.resolve())
    elif args.command == "status":
        status(args.root.resolve(), args.max_age_hours)
    elif args.command == "rollback":
        if not args.run_id:
            parser.error("rollback requires --run-id")
        rollback(args.root.resolve(), args.run_id)
    elif args.command == "backup":
        if not args.destination:
            parser.error("backup requires --destination")
        backup(args.root.resolve(), args.destination.resolve())
    else:
        if not args.backup:
            parser.error("restore requires --backup")
        restore(args.root.resolve(), args.backup.resolve())
