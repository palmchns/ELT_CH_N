"""dim_location: conformed location keys and fact relationships."""
import hashlib
import pandas as pd
from prefect import flow
from utils.quality import clean_dataframe, clean_table
from utils.schema import source_id

def _location_id(row):
    values = []
    for column in ("city", "state", "region", "country", "postalcode"):
        value = row.get(column)
        values.append("" if pd.isna(value) else str(value).strip().casefold())
    if not any(values):
        return pd.NA
    digest = hashlib.sha256("|".join(values).encode("utf-8")).hexdigest()[:16]
    return f"location:{digest}"


def build_landing(tables):
    location_columns = ["city", "state", "region", "country", "postalcode"]
    location_rows = []

    for table_name in ("dim_customer", "dim_employee"):
        dimension = tables[table_name].copy()
        for column in location_columns:
            if column not in dimension.columns:
                dimension[column] = pd.NA
        dimension["location_id"] = dimension.apply(_location_id, axis=1)
        tables[table_name] = dimension
        location_rows.append(dimension[["location_id"] + location_columns])

    dim_location = pd.concat(location_rows, ignore_index=True).dropna(subset=["location_id"])
    dim_location = dim_location.drop_duplicates(subset=["location_id"]).reset_index(drop=True)
    tables["dim_location"] = dim_location

    if "fact_order" not in tables:
        return tables

    fact = tables["fact_order"].copy()
    customer_locations = tables["dim_customer"][
        ["source_system", "customer_id", "location_id"]
    ].rename(columns={"location_id": "customer_location_id"})
    employee_locations = tables["dim_employee"][
        ["source_system", "employee_id", "location_id"]
    ].rename(columns={"location_id": "employee_location_id"})
    fact = fact.merge(customer_locations, on=["source_system", "customer_id"], how="left", validate="many_to_one")
    fact = fact.merge(employee_locations, on=["source_system", "employee_id"], how="left", validate="many_to_one")
    tables["fact_order"] = fact
    return tables


def stage_dataframe(dataframe):
    return clean_dataframe(dataframe, "dim_location")


@flow(name="flow_dim_location", log_prints=True, persist_result=False)
def flow_dim_location(source, storage):
    return clean_table(source, "dim_location", storage, processor=stage_dataframe)
