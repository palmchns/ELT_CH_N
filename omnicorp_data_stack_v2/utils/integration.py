"""Consolidate cleansed source models into the ready-to-query Integration star."""
import pandas as pd

from utils.quality import annotate_fact_quality, validate_model_integrity
from utils.schema import MODEL_TABLES


def consolidate_table(table, frames):
    result = pd.concat(frames, ignore_index=True)
    if table in {"dim_date", "dim_location"}:
        key = "date_id" if table == "dim_date" else "location_id"
        result = result.drop_duplicates(subset=[key]).reset_index(drop=True)
    return result


def consolidate_models(sources):
    tables = {table: consolidate_table(table, [source[table] for source in sources.values()]) for table in MODEL_TABLES}
    relationships, problems, mismatches = validate_model_integrity(tables)
    tables["fact_order"] = annotate_fact_quality(tables)
    if problems or tables["fact_order"]["quality_status"].eq("failed").any():
        raise ValueError("Integration model integrity failed: " + "; ".join(problems))
    return tables, relationships
