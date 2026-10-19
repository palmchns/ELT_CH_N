"""fact_order: raw source mapping and shared Staging flow."""
import pandas as pd
from prefect import flow
from utils.quality import clean_dataframe, clean_table
from utils.schema import source_id


def build_chinook(read_ingress):
    invoice = read_ingress("chinook", "invoice")
    invoice_line = read_ingress("chinook", "invoiceline")
    customer = read_ingress("chinook", "customer")

    fact = invoice_line.merge(invoice[["invoiceid", "customerid", "invoicedate"]], on="invoiceid", how="left")
    fact = fact.merge(customer[["customerid", "supportrepid"]], on="customerid", how="left", validate="many_to_one")
    fact["source_system"] = "chinook"
    fact["order_id"] = fact["invoiceid"].map(lambda x: source_id("chinook", x))
    fact["order_line_id"] = fact["invoicelineid"].map(lambda x: source_id("chinook", x))
    fact["customer_id"] = fact["customerid"].map(lambda x: source_id("chinook", x))
    fact["employee_id"] = fact["supportrepid"].map(lambda x: source_id("chinook", x))
    fact["source_customer_id"] = fact["customerid"]
    fact["source_employee_id"] = fact["supportrepid"]
    fact["source_product_id"] = fact["trackid"]
    fact["product_id"] = fact["trackid"].map(lambda x: source_id("chinook", x))
    fact["order_date"] = fact["invoicedate"]
    fact["discount"] = 0.0
    fact["amount"] = fact["unitprice"] * fact["quantity"]
    fact_order = fact[["source_system", "order_id", "order_line_id", "customer_id", "employee_id", "product_id", "order_date", "quantity", "unitprice", "discount", "amount", "source_customer_id", "source_employee_id", "source_product_id"]].rename(columns={"unitprice": "unit_price"})
    return fact_order


def build_northwind(read_ingress):
    orders = read_ingress("northwind", "orders")
    details = read_ingress("northwind", "order_details")

    fact = details.merge(orders, on="orderid", how="left", suffixes=("_line", "_order"))
    fact["source_system"] = "northwind"
    fact["order_id"] = fact["orderid"].map(lambda x: source_id("northwind", x))
    fact["order_line_id"] = fact.apply(lambda row: source_id("northwind", f"{row['orderid']}:{row['productid']}"), axis=1)
    fact["customer_id"] = fact["customerid"].map(lambda x: source_id("northwind", x))
    fact["employee_id"] = fact["employeeid"].map(lambda x: source_id("northwind", x))
    fact["product_id"] = fact["productid"].map(lambda x: source_id("northwind", x))
    fact["source_customer_id"] = fact["customerid"]
    fact["source_employee_id"] = fact["employeeid"]
    fact["source_product_id"] = fact["productid"]
    fact["order_date"] = fact["orderdate"]
    fact["amount"] = fact["unitprice"] * fact["quantity"] * (1 - fact["discount"])
    fact_order = fact[["source_system", "order_id", "order_line_id", "customer_id", "employee_id", "product_id", "order_date", "quantity", "unitprice", "discount", "amount", "source_customer_id", "source_employee_id", "source_product_id"]].rename(columns={"unitprice": "unit_price"})
    return fact_order


def stage_dataframe(dataframe):
    return clean_dataframe(dataframe, "fact_order")


@flow(name="flow_fact_order", log_prints=True, persist_result=False)
def flow_fact_order(source, storage):
    """Clean one source table; the parent flow validates and publishes the full model."""
    return clean_table(source, "fact_order", storage, processor=stage_dataframe)
