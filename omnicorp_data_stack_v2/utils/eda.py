"""Raw-source EDA and read-only previews using the production transformation code."""
import json
import os
from pathlib import Path
import sqlite3
from contextlib import closing

import pandas as pd
import pyarrow.parquet as pq

from utils.runtime import run_path, verify_snapshot
from utils.schema import MODEL_TABLES
from utils.config import SOURCES, INGRESS_TABLES
from utils.ingestion import download_source


RAW_INPUTS = {
    "dim_customer": {"chinook": ("customer",), "northwind": ("customers",)},
    "dim_employee": {"chinook": ("employee",), "northwind": ("employees",)},
    "dim_product": {"chinook": ("track", "album", "artist", "genre"), "northwind": ("products", "categories", "suppliers")},
    "dim_location": {"chinook": ("customer", "employee"), "northwind": ("customers", "employees")},
    "dim_date": {"chinook": ("invoice",), "northwind": ("orders",)},
    "fact_order": {source: tuple(tables) for source, tables in INGRESS_TABLES.items()},
}


def load_raw_inputs(table, db_paths=None, sources=None):
    """Read original SQLite values and headers before any pipeline stage runs."""
    if table not in RAW_INPUTS:
        raise ValueError(f"Unknown model table: {table}")
    db_paths = db_paths or {}
    result = {}
    for source in (sources if sources is not None else SOURCES):
        if source not in SOURCES:
            raise ValueError(f"Unknown source: {source}")
        downloaded = source not in db_paths
        path = Path(download_source(source, SOURCES[source]) if downloaded else db_paths[source]).resolve()
        try:
            with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as connection:
                for key in RAW_INPUTS[table][source]:
                    name = INGRESS_TABLES[source][key].replace('"', '""')
                    result[f"{source}.{key}"] = pd.read_sql_query(f'SELECT * FROM "{name}"', connection)
        finally:
            if downloaded:
                path.unlink(missing_ok=True)
    return result


def landing_preview(table, raw_inputs):
    """Build raw-valued Landing models without executing a single cleaning rule."""
    from flow import MODELS
    from utils.modeling import align_table, build_source_models
    if table not in MODEL_TABLES:
        raise ValueError(f"Unknown model table: {table}")

    def reader(source, key):
        frame = raw_inputs[f"{source}.{key}"].copy()
        frame.columns = [column.lower().replace(" ", "_") for column in frame.columns]
        return frame

    landing = {}
    for source in SOURCES:
        if not any(key.startswith(source + ".") for key in raw_inputs):
            continue
        if table == "fact_order":
            bundle = build_source_models(source, reader)
        elif table == "dim_date":
            key, column = ("invoice", "invoicedate") if source == "chinook" else ("orders", "orderdate")
            bundle = {table: MODELS[table].build_landing(pd.DataFrame({"order_date": reader(source, key)[column]}))}
        elif table == "dim_location":
            bundle = {name: getattr(MODELS[name], f"build_{source}")(reader) for name in ("dim_customer", "dim_employee")}
            bundle = MODELS[table].build_landing(bundle)
        else:
            bundle = {table: getattr(MODELS[table], f"build_{source}")(reader)}
        landing[source] = {name: align_table(name, frame) for name, frame in bundle.items()}
    if not landing:
        raise ValueError("No source inputs were provided")
    return landing


def staging_preview(table, landing_models):
    """Apply production rules only when the notebook reaches its Staging cell."""
    from flow import MODELS
    from utils.quality import annotate_fact_quality, validate_model_integrity
    staging, metadata, checks = {}, [], []
    for source, bundle in landing_models.items():
        cleaned = {}
        for name, frame in bundle.items():
            cleaned[name], summary = MODELS[name].stage_dataframe(frame)
            if name == table:
                metadata.extend({**item, "source_system": source} for item in summary)
        if table == "fact_order":
            relationships, _, _ = validate_model_integrity(cleaned)
            checks.extend({**item, "source_system": source, "table": name}
                          for name, items in relationships.items() for item in items)
            cleaned[table] = annotate_fact_quality(cleaned)
        staging[source] = cleaned[table]
    return {"staging": staging, "metadata": pd.DataFrame(metadata), "relationships": pd.DataFrame(checks)}


def integration_preview(table, staging):
    from utils.integration import consolidate_table
    return consolidate_table(table, list(staging.values()))


def preview_layers(table, raw_inputs):
    """Convenience API for all read-only previews; notebooks invoke each phase separately."""
    landing_models = landing_preview(table, raw_inputs)
    result = staging_preview(table, landing_models)
    return {"landing": {source: bundle[table] for source, bundle in landing_models.items()},
            **result, "integration": integration_preview(table, result["staging"])}


def load_published_table(table, root=None, run_id=None):
    """Resolve the pointer once and return a table plus its snapshot manifest."""
    if table not in MODEL_TABLES:
        raise ValueError(f"Unknown model table: {table}")
    root = Path(root if root is not None else os.getenv("OBS_DIR", "./obs_data")).resolve()
    if run_id is None:
        try:
            run_id = json.loads((root / "latest.json").read_text())["run_id"]
        except FileNotFoundError as error:
            raise FileNotFoundError(f"No published snapshot at {root}. Set OBS_DIR or run the pipeline first.") from error
    manifest = verify_snapshot(root, run_id, hashes=False)
    artifact = manifest["artifacts"][table]
    path = run_path(root, run_id) / artifact["path"]
    # ParquetFile avoids inferring extra Hive columns from the directory names.
    dataframe = pq.ParquetFile(path).read().to_pandas()
    return dataframe, manifest
