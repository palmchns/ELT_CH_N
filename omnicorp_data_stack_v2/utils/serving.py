"""Publish a verified Integration snapshot transactionally to PostgreSQL for Metabase."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from utils.schema import MODEL_TABLES, MODEL_COLUMNS, MODEL_KEYS, FACT_REFERENCES
from utils.runtime import event, run_path, verify_snapshot, writer_lock, write_json


def secret(name):
    directory = Path(os.getenv("BI_SECRETS_DIR", "/run/secrets"))
    value = (directory / name).read_text().strip()
    if not value:
        raise ValueError(f"Empty secret: {name}")
    return value


def connection_options(user="pipeline_writer", database="omnicorp", password_name="bi_writer"):
    return dict(host=os.getenv("BI_HOST", "warehouse"), port=int(os.getenv("BI_PORT", "5432")),
                dbname=database, user=user, password=secret(password_name), connect_timeout=10)


def initialize():
    """Idempotently create dedicated application roles and databases on the BI server."""
    import psycopg
    from psycopg import sql
    with psycopg.connect(**connection_options(os.getenv("BI_ADMIN_USER", "warehouse_admin"), "postgres", "bi_postgres_admin"), autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_lock(7843021)")
            try:
                for role, password_name in [("pipeline_writer", "bi_writer"), ("metabase_reader", "bi_reader"), ("metabase_app", "metabase_app")]:
                    cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
                    verb = "ALTER" if cursor.fetchone() else "CREATE"
                    cursor.execute(sql.SQL(verb + " ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD {}").format(
                        sql.Identifier(role), sql.Literal(secret(password_name))))
                for database, owner in [("omnicorp", "pipeline_writer"), ("metabase", "metabase_app")]:
                    cursor.execute("SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = %s", (database,))
                    row = cursor.fetchone()
                    if row is None:
                        cursor.execute(sql.SQL("CREATE DATABASE {} OWNER {}").format(sql.Identifier(database), sql.Identifier(owner)))
                    elif row[0] != owner:
                        raise ValueError(f"Existing database has an unexpected owner: {database}")
                cursor.execute("REVOKE CONNECT ON DATABASE omnicorp FROM PUBLIC")
                cursor.execute("GRANT CONNECT ON DATABASE omnicorp TO pipeline_writer, metabase_reader")
                cursor.execute("REVOKE CONNECT ON DATABASE metabase FROM PUBLIC")
                cursor.execute("GRANT CONNECT ON DATABASE metabase TO metabase_app")
            finally:
                cursor.execute("SELECT pg_advisory_unlock(7843021)")
    event("bi_initialized")


def postgres_type(dtype):
    if pa.types.is_dictionary(dtype):
        return postgres_type(dtype.value_type)
    if pa.types.is_integer(dtype):
        return "BIGINT"
    if pa.types.is_floating(dtype):
        return "DOUBLE PRECISION"
    if pa.types.is_date(dtype):
        return "DATE"
    if pa.types.is_timestamp(dtype):
        return "TIMESTAMP WITH TIME ZONE" if dtype.tz else "TIMESTAMP WITHOUT TIME ZONE"
    if pa.types.is_boolean(dtype):
        return "BOOLEAN"
    if pa.types.is_decimal(dtype):
        return f"NUMERIC({dtype.precision},{dtype.scale})"
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype) or pa.types.is_null(dtype):
        return "TEXT"
    raise ValueError(f"Unsupported PostgreSQL column type: {dtype}")


def _create_table(cursor, table, arrow_table):
    from psycopg import sql
    columns = [sql.SQL("{} {}").format(sql.Identifier(field.name), sql.SQL(postgres_type(field.type))) for field in arrow_table.schema]
    columns.append(sql.SQL("PRIMARY KEY ({})").format(sql.Identifier(MODEL_KEYS[table])))
    references = FACT_REFERENCES if table == "fact_order" else ({"manager_id": ("dim_employee", "employee_id")} if table == "dim_employee" else {})
    for column, (dimension, key) in references.items():
        columns.append(sql.SQL("FOREIGN KEY ({}) REFERENCES integration.{} ({}) DEFERRABLE INITIALLY DEFERRED").format(
            sql.Identifier(column), sql.Identifier(dimension), sql.Identifier(key)))
    cursor.execute(sql.SQL("CREATE TABLE IF NOT EXISTS integration.{} ({})").format(sql.Identifier(table), sql.SQL(", ").join(columns)))
    cursor.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema='integration' AND table_name=%s ORDER BY ordinal_position", (table,))
    expected = [(field.name, postgres_type(field.type).split("(")[0].lower()) for field in arrow_table.schema]
    if cursor.fetchall() != expected:
        raise ValueError(f"Serving schema migration required for {table}")


def _copy_table(cursor, table, frame):
    from psycopg import sql
    command = sql.SQL("COPY integration.{} ({}) FROM STDIN").format(
        sql.Identifier(table), sql.SQL(", ").join(map(sql.Identifier, frame.columns)))
    with cursor.copy(command) as copy:
        for row in frame.itertuples(index=False, name=None):
            copy.write_row(tuple(None if pd.isna(value) else value.item() if hasattr(value, "item") else value for value in row))


def publish(root, run_id=None):
    import psycopg
    from psycopg import sql
    from psycopg.types.json import Jsonb
    from utils.quality import validate_model_integrity, annotate_fact_quality

    root = Path(root).resolve()
    if run_id is None:
        with writer_lock(root):
            run_id = json.loads((root / "latest.json").read_text())["run_id"]
            return publish(root, run_id)
    manifest = verify_snapshot(root, run_id)
    if manifest.get("published_zone") != "integration" or manifest["schema_version"] != 2:
        raise ValueError("Metabase serving requires a version 2 Integration snapshot; rerun the pipeline for legacy data")
    fingerprint = hashlib.sha256(json.dumps(manifest["artifacts"], sort_keys=True).encode()).hexdigest()
    tables, metadata = {}, []
    for table, artifact in manifest["artifacts"].items():
        path = run_path(root, run_id) / artifact["path"]
        arrow = pq.ParquetFile(path).read()
        if arrow.column_names != MODEL_COLUMNS[table] + (["quality_status", "quality_issues"] if table == "fact_order" else []):
            raise ValueError(f"Unexpected Integration schema for {table}")
        tables[table] = arrow
        footer = pq.read_metadata(path).metadata or {}
        for item in json.loads(footer.get(b"quality_summary", b"[]")):
            metadata.append((table, item.get("source_system"), item["col"], item["role"], item["status"], int(item["count"])))
    frames = {table: data.to_pandas() for table, data in tables.items()}
    _, problems, _ = validate_model_integrity(frames)
    if problems or annotate_fact_quality(frames).quality_status.eq("failed").any():
        raise ValueError("Refusing to serve an invalid Integration model")

    with psycopg.connect(**connection_options()) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL lock_timeout = '30s'")
            cursor.execute("SELECT pg_advisory_xact_lock(7843022)")
            cursor.execute("CREATE SCHEMA IF NOT EXISTS integration")
            cursor.execute("CREATE TABLE IF NOT EXISTS integration._pipeline_run (singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK(singleton), run_id TEXT NOT NULL, manifest_digest TEXT NOT NULL, published_at TIMESTAMPTZ NOT NULL, loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(), row_counts JSONB NOT NULL)")
            cursor.execute("SELECT run_id, manifest_digest FROM integration._pipeline_run WHERE singleton")
            current = cursor.fetchone()
            if current and current[0] == run_id:
                if current[1] != fingerprint:
                    raise ValueError("Published snapshot changed after its first serving load")
                write_json(root / "serving.json", {"run_id": run_id, "status": "complete", "loaded_at": datetime.now(timezone.utc).isoformat()})
                event("bi_already_current", run_id=run_id)
                return run_id
            for table in MODEL_TABLES:
                _create_table(cursor, table, tables[table])
            cursor.execute("CREATE TABLE IF NOT EXISTS integration.quality_summary (table_name TEXT, source_system TEXT, column_name TEXT, rule TEXT, status TEXT, check_count BIGINT)")
            # DELETE preserves MVCC readers. All six tables switch in one transaction.
            for table in ("fact_order", "dim_employee", "dim_customer", "dim_product", "dim_location", "dim_date"):
                cursor.execute(sql.SQL("DELETE FROM integration.{}").format(sql.Identifier(table)))
            for table in MODEL_TABLES:
                _copy_table(cursor, table, frames[table])
            cursor.execute("DELETE FROM integration.quality_summary")
            if metadata:
                cursor.executemany("INSERT INTO integration.quality_summary VALUES (%s,%s,%s,%s,%s,%s)", metadata)
            cursor.execute("INSERT INTO integration._pipeline_run (singleton,run_id,manifest_digest,published_at,row_counts) VALUES (TRUE,%s,%s,%s,%s) ON CONFLICT(singleton) DO UPDATE SET run_id=EXCLUDED.run_id, manifest_digest=EXCLUDED.manifest_digest, published_at=EXCLUDED.published_at, loaded_at=now(), row_counts=EXCLUDED.row_counts",
                           (run_id, fingerprint, manifest["completed_at"], Jsonb({table: len(frame) for table, frame in frames.items()})))
            cursor.execute("GRANT USAGE ON SCHEMA integration TO metabase_reader")
            cursor.execute("GRANT SELECT ON ALL TABLES IN SCHEMA integration TO metabase_reader")
            cursor.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA integration GRANT SELECT ON TABLES TO metabase_reader")
    write_json(root / "serving.json", {"run_id": run_id, "status": "complete", "loaded_at": datetime.now(timezone.utc).isoformat()})
    event("bi_published", run_id=run_id, fact_rows=len(frames["fact_order"]))
    return run_id


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["init", "publish"])
    parser.add_argument("--root", type=Path, default=Path(os.getenv("OBS_DIR", "./obs_data")))
    parser.add_argument("--run-id")
    args = parser.parse_args()
    if args.command == "init":
        initialize()
    else:
        publish(args.root, args.run_id)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        event("bi_command_failed", error_type=type(error).__name__)
        raise SystemExit(1)
