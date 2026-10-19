"""Shared column rules, quality summaries, and model integrity checks."""
import duckdb
import pandas as pd
from utils.schema import MODEL_TABLES, MODEL_KEYS, FACT_REFERENCES
from monadsquishy.monadsquishy import SquishyEngine
from monadsquishy.rules import (
    flexible_date_parser,
    must_exist,
    normalize_optional_whitespace,
    to_float,
    transform_to_uppercase,
    validate_contact_number,
    validate_email,
    validate_non_negative,
    validate_numeric_only,
    validate_optional_whole_number,
    validate_positive,
    validate_unit_interval,
)

def summarize_logs(logs):
    if logs is None or logs.empty:
        return []
    if "count" in logs.columns:
        return logs.to_dict(orient="records")
    fields = ["col", "step", "role", "role_type", "status"]
    return logs.groupby(fields, dropna=False).size().reset_index(name="count").to_dict(orient="records")


def staging_config(dataframe, table):
    config = []
    required_ids = {
        "dim_date": {"date_id"},
        "dim_location": {"location_id"},
        "dim_customer": {"source_system", "customer_id", "customerid"},
        "dim_employee": {"source_system", "employee_id", "employeeid"},
        "dim_product": {"source_system", "product_id"},
        "fact_order": {"source_system", "order_id", "order_line_id", "customer_id", "product_id", "date_id", "source_customer_id", "source_product_id"},
    }[table]
    numeric_columns = {
        "quantity", "unit_price", "discount", "amount",
        "calendar_year", "calendar_month", "calendar_day", "quarter", "day_of_week",
    }
    text_columns = {
        "company", "contact_name", "first_name", "last_name", "title", "product_name",
        "genre", "artist", "album", "category", "supplier",
        "address", "city", "state", "region", "country", "postalcode",
        "manager_id", "employee_id", "customer_location_id", "employee_location_id",
        "day_name", "month_name",
        "source_employee_id",
    }
    for column in dataframe.columns:
        if column in {"date_id", "order_date", "hire_date"}:
            pipeline = [must_exist, flexible_date_parser] if column in required_ids or column == "order_date" else [flexible_date_parser]
            config.append({"target": column, "source": column, "type": "date", "pipeline": pipeline})
        elif column == "employeeid":
            config.append({"target": column, "source": column, "type": "int", "pipeline": [must_exist, validate_optional_whole_number]})
        elif column in {"reportsto", "trackid", "productid"}:
            config.append({"target": column, "source": column, "type": "int", "pipeline": [validate_optional_whole_number]})
        elif column in required_ids:
            config.append({"target": column, "source": column, "pipeline": [must_exist]})
        elif column == "country":
            config.append({"target": column, "source": column, "pipeline": [transform_to_uppercase]})
        elif column in numeric_columns:
            pipeline = [validate_numeric_only, to_float]
            if column == "quantity":
                pipeline.append(validate_positive)
            elif column == "discount":
                pipeline.append(validate_unit_interval)
            elif column in {"unit_price", "amount"}:
                pipeline.append(validate_non_negative)
            config.append({"target": column, "source": column, "type": "float", "pipeline": pipeline})
        elif column in {"phone", "fax"}:
            config.append({"target": column, "source": column, "pipeline": [normalize_optional_whitespace, validate_contact_number]})
        elif column == "email":
            config.append({"target": column, "source": column, "pipeline": [normalize_optional_whitespace, validate_email]})
        elif column in text_columns:
            config.append({"target": column, "source": column, "pipeline": [normalize_optional_whitespace]})
    uncovered = set(dataframe.columns) - {item["target"] for item in config}
    if uncovered:
        raise ValueError(f"Monasquishy rules missing for {table}: {sorted(uncovered)}")
    return config


def validate_model_integrity(tables):
    """Summarize PK/FK relationships; null FKs are source blanks, not orphan keys."""
    problems = []
    relationships = {table: [] for table in MODEL_TABLES}

    def examples(values):
        return [str(value) for value in values.dropna().drop_duplicates().head(3)]

    keys = MODEL_KEYS
    for table, key in keys.items():
        values = tables[table][key]
        null_count = int(values.isna().sum())
        duplicate_count = int(values.dropna().duplicated().sum())
        duplicate_keys = values[values.notna() & values.duplicated(keep=False)]
        relationships[table].append({
            "type": "PK", "column": key, "row_count": len(values),
            "null_count": null_count, "duplicate_count": duplicate_count,
            "sample_keys": examples(values),
            "sample_duplicate_keys": examples(duplicate_keys),
            "status": "passed" if null_count == 0 and duplicate_count == 0 else "failed",
        })
        if null_count or duplicate_count:
            problems.append(f"{table}.{key}: {null_count} null, {duplicate_count} duplicate keys")

    fact = tables["fact_order"]
    references = FACT_REFERENCES
    for column, (dimension, key) in references.items():
        values = fact[column]
        non_null = values.dropna()
        missing_keys = non_null[~non_null.isin(tables[dimension][key])]
        relationships["fact_order"].append({
            "type": "FK", "column": column, "references": f"{dimension}.{key}",
            "row_count": len(values), "null_count": int(values.isna().sum()),
            "missing_count": len(missing_keys),
            "sample_matched_keys": examples(non_null[non_null.isin(tables[dimension][key])]),
            "sample_missing_keys": examples(missing_keys),
            "status": "passed" if missing_keys.empty else "failed",
        })
        if not missing_keys.empty:
            problems.append(f"fact_order.{column}: {len(missing_keys)} missing {dimension} keys; examples={examples(missing_keys)}")

    employees = tables["dim_employee"]
    managers = employees["manager_id"]
    non_null_managers = managers.dropna()
    missing_managers = non_null_managers[~non_null_managers.isin(employees["employee_id"])]
    relationships["dim_employee"].append({
        "type": "FK", "column": "manager_id", "references": "dim_employee.employee_id",
        "row_count": len(managers), "null_count": int(managers.isna().sum()),
        "missing_count": len(missing_managers),
        "sample_matched_keys": examples(non_null_managers[non_null_managers.isin(employees["employee_id"])]),
        "sample_missing_keys": examples(missing_managers),
        "status": "passed" if missing_managers.empty else "failed",
    })
    if not missing_managers.empty:
        problems.append(f"dim_employee.manager_id: {len(missing_managers)} missing employee keys; examples={examples(missing_managers)}")

    expected = fact["quantity"] * fact["unit_price"] * (1 - fact["discount"])
    amount_mismatch = int(((fact["amount"] - expected).abs() > 0.01).sum())
    if amount_mismatch:
        problems.append(f"fact_order.amount: {amount_mismatch} mismatches")
    date_mismatch = int((fact["date_id"].notna() & fact["order_date"].notna() & fact["date_id"].ne(fact["order_date"])).sum())
    if date_mismatch:
        problems.append(f"fact_order.date_id: {date_mismatch} mismatches with order_date")
    return relationships, problems, amount_mismatch


def annotate_fact_quality(tables):
    """Keep every fact row and mark broken links or unusable required values."""
    fact = tables["fact_order"].copy()
    issues = pd.Series("", index=fact.index, dtype="string")

    def mark(mask, code):
        current = issues.loc[mask]
        issues.loc[mask] = current.apply(lambda value: f"{value}|{code}" if value else code)

    for column in ("source_system", "order_id", "order_line_id", "customer_id", "product_id", "date_id", "order_date", "quantity", "unit_price", "discount", "amount"):
        mark(fact[column].isna(), f"null_{column}")
    mark(fact["order_line_id"].notna() & fact["order_line_id"].duplicated(keep=False), "duplicate_order_line_id")

    references = FACT_REFERENCES
    for column, (dimension, key) in references.items():
        dimension_keys = tables[dimension][key]
        mark(fact[column].notna() & ~fact[column].isin(dimension_keys), f"missing_{dimension}")
        duplicate_keys = dimension_keys[dimension_keys.notna() & dimension_keys.duplicated(keep=False)]
        if not duplicate_keys.empty:
            mark(fact[column].notna() & fact[column].isin(duplicate_keys), f"duplicate_{dimension}")

    mark(fact["source_employee_id"].notna() & fact["employee_id"].isna(), "missing_dim_employee")
    mark(fact["date_id"].notna() & fact["order_date"].notna() & fact["date_id"].ne(fact["order_date"]), "date_mismatch")

    expected = fact["quantity"] * fact["unit_price"] * (1 - fact["discount"])
    mark((fact["amount"] - expected).abs() > 0.01, "amount_mismatch")
    fact["quality_status"] = issues.eq("").map({True: "passed", False: "failed"})
    fact["quality_issues"] = issues.mask(issues.eq(""), pd.NA)
    return fact



def clean_dataframe(dataframe, table):
    """Apply the same rules in notebooks and production flows."""
    engine = SquishyEngine(staging_config(dataframe, table), dataframe)
    cleaned = engine.run_batch()
    return cleaned, summarize_logs(engine.logs)


def clean_table(source, table, storage, processor=None):
    path = storage.latest_parquet("landing", source, table)
    with duckdb.connect() as con:
        dataframe = con.execute("SELECT * FROM read_parquet(?, hive_partitioning=false)", [path]).df()
    cleaned, summary = (processor or (lambda frame: clean_dataframe(frame, table)))(dataframe)
    failed_rules = [item for item in summary if item["status"] == "failed"]
    failures = sum(item["count"] for item in failed_rules)
    if failures:
        examples = "; ".join(
            f"{item['col']}.{item['role']}: {item['count']} failures"
            for item in failed_rules[:5]
        )
        print(f"⚠️ Monasquishy {source}.{table}: {failures} failed rule checks; {examples}")
    return cleaned, summary
