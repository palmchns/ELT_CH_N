"""dim_customer: raw source mapping and shared Staging flow."""
import pandas as pd
from prefect import flow
from utils.quality import clean_dataframe, clean_table
from utils.schema import source_id


def build_chinook(read_ingress):
    customer = read_ingress("chinook", "customer")

    dim_customer = customer.rename(columns={"firstname": "first_name", "lastname": "last_name"})
    dim_customer["source_system"] = "chinook"
    dim_customer["customer_id"] = dim_customer["customerid"].map(lambda x: source_id("chinook", x))
    dim_customer = dim_customer[["source_system", "customer_id", "customerid", "first_name", "last_name", "company", "address", "city", "state", "country", "postalcode", "phone", "fax", "email"]]

    return dim_customer


def build_northwind(read_ingress):
    customer = read_ingress("northwind", "customers")

    dim_customer = customer.rename(columns={"companyname": "company", "contactname": "contact_name"})
    dim_customer["source_system"] = "northwind"
    dim_customer["customer_id"] = dim_customer["customerid"].map(lambda x: source_id("northwind", x))
    if "email" not in dim_customer.columns:
        dim_customer["email"] = pd.NA
    dim_customer = dim_customer[["source_system", "customer_id", "customerid", "contact_name", "company", "address", "city", "region", "country", "postalcode", "phone", "fax", "email"]]

    return dim_customer


def stage_dataframe(dataframe):
    return clean_dataframe(dataframe, "dim_customer")


@flow(name="flow_dim_customer", log_prints=True, persist_result=False)
def flow_dim_customer(source, storage):
    """Clean one source table; the parent flow validates and publishes the full model."""
    return clean_table(source, "dim_customer", storage, processor=stage_dataframe)
