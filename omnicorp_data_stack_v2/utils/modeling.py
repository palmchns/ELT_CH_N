"""Build a raw-valued Landing star with foreign keys looked up from dimensions."""
import pandas as pd

from utils.schema import MODEL_COLUMNS, align_model_schema


def attach_dimension_keys(tables):
    fact = tables["fact_order"].copy()
    for column, table in (("customer_id", "dim_customer"), ("employee_id", "dim_employee"), ("product_id", "dim_product")):
        lookup = tables[table][[column]].dropna().rename(columns={column: "__dimension_id"})
        fact = fact.merge(lookup, left_on=column, right_on="__dimension_id", how="left", validate="many_to_one")
        fact[column] = fact.pop("__dimension_id")
    tables["fact_order"] = fact
    return tables


def add_conformed_dimensions(tables):
    from flow import MODELS
    tables = attach_dimension_keys(tables)
    tables = MODELS["dim_location"].build_landing(tables)
    tables["dim_date"] = MODELS["dim_date"].build_landing(tables["fact_order"])
    fact = tables["fact_order"]
    lookup = tables["dim_date"][["date_id"]]
    tables["fact_order"] = fact.merge(lookup, left_on="order_date", right_on="date_id", how="left", validate="many_to_one")
    return tables


def build_source_models(source, reader):
    from flow import MODELS
    tables = {table: getattr(MODELS[table], f"build_{source}")(reader)
              for table in ("dim_customer", "dim_employee", "dim_product", "fact_order")}
    return align_model_schema(add_conformed_dimensions(tables))


def align_table(table, dataframe):
    result = dataframe.copy()
    for column in MODEL_COLUMNS[table]:
        if column not in result:
            result[column] = pd.NA
    return result[MODEL_COLUMNS[table]]
