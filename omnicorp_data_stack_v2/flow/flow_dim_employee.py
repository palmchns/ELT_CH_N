"""dim_employee: raw source mapping and shared Staging flow."""
import pandas as pd
from prefect import flow
from utils.quality import clean_dataframe, clean_table
from utils.schema import source_id


def build_chinook(read_ingress):
    employee = read_ingress("chinook", "employee")

    dim_employee = employee.rename(columns={"firstname": "first_name", "lastname": "last_name"})
    dim_employee["source_system"] = "chinook"
    dim_employee["employee_id"] = dim_employee["employeeid"].map(lambda x: source_id("chinook", x))
    dim_employee["manager_id"] = dim_employee["reportsto"].map(lambda x: source_id("chinook", x) if pd.notna(x) else pd.NA)
    dim_employee = dim_employee.rename(columns={"hiredate": "hire_date"})
    dim_employee = dim_employee[["source_system", "employee_id", "employeeid", "first_name", "last_name", "title", "reportsto", "manager_id", "hire_date", "address", "city", "state", "country", "postalcode", "phone", "fax", "email"]]

    return dim_employee


def build_northwind(read_ingress):
    employee = read_ingress("northwind", "employees")

    dim_employee = employee.rename(columns={"firstname": "first_name", "lastname": "last_name"})
    dim_employee["source_system"] = "northwind"
    dim_employee["employee_id"] = dim_employee["employeeid"].map(lambda x: source_id("northwind", x))
    dim_employee["manager_id"] = dim_employee["reportsto"].map(lambda x: source_id("northwind", x) if pd.notna(x) else pd.NA)
    dim_employee = dim_employee.rename(columns={"hiredate": "hire_date", "homephone": "phone"})
    dim_employee = dim_employee[["source_system", "employee_id", "employeeid", "first_name", "last_name", "title", "reportsto", "manager_id", "hire_date", "address", "city", "region", "country", "postalcode", "phone"]]

    return dim_employee


def stage_dataframe(dataframe):
    return clean_dataframe(dataframe, "dim_employee")


@flow(name="flow_dim_employee", log_prints=True, persist_result=False)
def flow_dim_employee(source, storage):
    """Clean one source table; the parent flow validates and publishes the full model."""
    return clean_table(source, "dim_employee", storage, processor=stage_dataframe)
