"""Shared orchestration; table-specific logic lives in flow/flow_<table>.py."""
import json
import os
import sqlite3
import tempfile
import duckdb
import pandas as pd
import pyarrow.parquet as pq
import requests
from prefect import flow, task

from flow import MODELS, MODEL_FLOWS
from utils.config import OBS_DIR, TODAY, YEAR, MONTH, DAY, RUN_HHMMSS, SOURCES, ZONE_CODES, INGRESS_TABLES
from utils.schema import MODEL_TABLES, MODEL_COLUMNS, align_model_schema, source_id
from utils.storage import LakeStorage
from utils.ingestion import download_source
from utils.quality import summarize_logs, staging_config, validate_model_integrity, annotate_fact_quality
from utils.modeling import add_conformed_dimensions
from utils.integration import consolidate_models


def _storage():
    return LakeStorage(str(OBS_DIR), YEAR, MONTH, DAY, RUN_HHMMSS)


def hive_dir(zone, table):
    return _storage().hive_dir(zone, table)


def parquet_path(zone, source, table):
    return _storage().parquet_path(zone, source, table)


def integration_path(table):
    return _storage().integration_path(table)


def read_ingress(source, table):
    return _storage().read_ingress(source, table)


def latest_parquet(zone, source, table):
    return _storage().latest_parquet(zone, source, table)


def write_parquet(dataframe, path, quality_summary=None, quality_context=None, relationship_summary=None):
    return _storage().write_parquet(dataframe, path, quality_summary, quality_context, relationship_summary)


@task(log_prints=True, retries=2)
def task1_ingress():
    print("📥 [Ingress] Extracting Chinook and Northwind to Raw Parquet...")
    for source, url in SOURCES.items():
        db_path = download_source(source, url)
        con = None
        try:
            con = sqlite3.connect(db_path)
            tables = pd.read_sql_query(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
                con,
            )["name"].tolist()
            for table in tables:
                df = pd.read_sql_query(f'SELECT * FROM "{table}"', con)
                df.columns = [column.lower().replace(" ", "_") for column in df.columns]
                # Preserve source spelling/casing in the ingress filename.
                path = parquet_path("ingress", source, table)
                write_parquet(df, path)
                print(f"✅ Ingress: {source}.{table} -> {path}")
        finally:
            if con is not None:
                con.close()
            os.unlink(db_path)


def build_chinook_landing():
    return {
        "dim_customer": MODELS["dim_customer"].build_chinook(read_ingress),
        "dim_employee": MODELS["dim_employee"].build_chinook(read_ingress),
        "dim_product": MODELS["dim_product"].build_chinook(read_ingress),
        "fact_order": MODELS["fact_order"].build_chinook(read_ingress),
    }


def build_northwind_landing():
    return {
        "dim_customer": MODELS["dim_customer"].build_northwind(read_ingress),
        "dim_employee": MODELS["dim_employee"].build_northwind(read_ingress),
        "dim_product": MODELS["dim_product"].build_northwind(read_ingress),
        "fact_order": MODELS["fact_order"].build_northwind(read_ingress),
    }


@task(log_prints=True)
def task2_landing():
    print("🏗️ [Landing] Mapping both sources to common star schema...")
    builders = {"chinook": build_chinook_landing, "northwind": build_northwind_landing}
    for source, builder in builders.items():
        for table, dataframe in align_model_schema(add_conformed_dimensions(builder())).items():
            path = parquet_path("landing", source, table)
            write_parquet(dataframe, path)
            print(f"✅ Landing: {source}.{table} rows={len(dataframe)}")


@flow(name="OmniCorp staging", log_prints=True)
def task3_staging():
    print("🧼 [Staging] Applying Monasquishy to both sources...")
    for source in SOURCES:
        staged = {}
        summaries = {}
        for table in MODEL_TABLES:
            staged[table], summaries[table] = MODEL_FLOWS[table](source, _storage())
        relationships, problems, amount_mismatch = validate_model_integrity(staged)
        staged["fact_order"] = annotate_fact_quality(staged)
        for table in MODEL_TABLES:
            output = parquet_path("staging", source, table)
            failed_rules = sum(item["count"] for item in summaries[table] if item["status"] == "failed")
            flagged_rows = int(staged[table]["quality_status"].eq("failed").sum()) if table == "fact_order" else 0
            write_parquet(staged[table], output, summaries[table], {
                "schema_version": 1,
                "source_system": source,
                "table": table,
                "run_date": TODAY.date().isoformat(),
                "row_count": len(staged[table]),
                "quality_status": "failed" if failed_rules or flagged_rows or any(item["status"] == "failed" for item in relationships[table]) or (table == "fact_order" and amount_mismatch) else "passed",
                "failed_rule_checks": failed_rules,
                "flagged_rows": flagged_rows,
                "amount_mismatch_count": amount_mismatch if table == "fact_order" else 0,
            }, relationship_summary=relationships[table])
            print(f"✅ Staging: {source}.{table} rows={len(staged[table])} footer=quality_summary,relationship_summary")
        if problems:
            print(f"⚠️ {source} model integrity: " + "; ".join(problems))


@task(log_prints=True)
def task4_integration():
    print("📚 [Integration] Publishing the consolidated, ready-to-use star schema...")
    sources = {}
    summaries = {table: [] for table in MODEL_TABLES}
    for source in SOURCES:
        sources[source] = {}
        for table in MODEL_TABLES:
            path = latest_parquet("staging", source, table)
            with duckdb.connect() as con:
                sources[source][table] = con.execute("SELECT * FROM read_parquet(?, hive_partitioning=false)", [path]).df()
            metadata = pq.read_metadata(path).metadata or {}
            summaries[table].extend({**item, "source_system": source} for item in json.loads(metadata[b"quality_summary"]))
    tables, relationships = consolidate_models(sources)
    for table in MODEL_TABLES:
        failures = sum(item["count"] for item in summaries[table] if item["status"] == "failed")
        write_parquet(tables[table], integration_path(table), summaries[table], {
            "schema_version": 2, "zone": "integration", "source_system": "conformed", "table": table,
            "run_date": TODAY.date().isoformat(), "row_count": len(tables[table]),
            "source_row_counts": {source: len(model[table]) for source, model in sources.items()},
            "failed_rule_checks": failures, "quality_status": "failed" if failures else "passed",
            "flagged_rows": 0, "amount_mismatch_count": 0,
        }, relationship_summary=relationships[table])
        print(f"✅ Integration: {table} rows={len(tables[table])}")


@flow(name="OmniCorp V2 ELT Pipeline")
def main_flow():
    task1_ingress()
    task2_landing()
    task3_staging()
    check_staging_quality()
    task4_integration()


@task(log_prints=True)
def check_staging_quality():
    from pathlib import Path
    from utils.runtime import quality_gate
    threshold = int(os.getenv("MAX_FAILED_RULE_CHECKS", "0"))
    if threshold < 0:
        raise ValueError("MAX_FAILED_RULE_CHECKS must be non-negative")
    return quality_gate(Path(OBS_DIR), threshold)
