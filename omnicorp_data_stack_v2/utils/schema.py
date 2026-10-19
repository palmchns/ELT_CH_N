"""Shared model columns and source-qualified identifiers."""
import pandas as pd

MODEL_TABLES = (
    "dim_date",
    "dim_location",
    "dim_customer",
    "dim_employee",
    "dim_product",
    "fact_order",
)
MODEL_KEYS = {
    "dim_date": "date_id", "dim_location": "location_id", "dim_customer": "customer_id",
    "dim_employee": "employee_id", "dim_product": "product_id", "fact_order": "order_line_id",
}
FACT_REFERENCES = {
    "date_id": ("dim_date", "date_id"), "customer_id": ("dim_customer", "customer_id"),
    "employee_id": ("dim_employee", "employee_id"), "product_id": ("dim_product", "product_id"),
    "customer_location_id": ("dim_location", "location_id"), "employee_location_id": ("dim_location", "location_id"),
}
MODEL_COLUMNS = {
    "dim_date": [
        "date_id", "calendar_year", "calendar_month", "calendar_day",
        "quarter", "day_of_week", "day_name", "month_name",
    ],
    "dim_location": ["location_id", "city", "state", "region", "country", "postalcode"],
    "dim_customer": [
        "source_system", "customer_id", "customerid", "first_name", "last_name", "contact_name",
        "company", "address", "city", "state", "region", "country", "postalcode",
        "phone", "fax", "email",
    ],
    "dim_employee": [
        "source_system", "employee_id", "employeeid", "first_name", "last_name", "title",
        "reportsto", "manager_id", "hire_date", "address", "city", "state",
        "region", "country", "postalcode", "phone", "fax", "email",
    ],
    "dim_product": [
        "source_system", "product_id", "trackid", "productid", "product_name", "genre", "artist",
        "album", "category", "supplier", "unit_price",
    ],
    "fact_order": [
        "source_system", "order_id", "order_line_id", "customer_id", "employee_id", "product_id",
        "customer_location_id", "employee_location_id", "date_id", "order_date",
        "source_customer_id", "source_employee_id", "source_product_id",
        "quantity", "unit_price", "discount", "amount",
    ],
}


def source_id(source, value):
    if pd.isna(value):
        return pd.NA
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    return f"{source}:{value}"


def align_model_schema(tables):
    """Keep identical column names/order across source files and pipeline zones."""
    for table, columns in MODEL_COLUMNS.items():
        dataframe = tables[table].copy()
        for column in columns:
            if column not in dataframe.columns:
                dtype = "Int64" if column in {"trackid", "productid"} else "string"
                dataframe[column] = pd.Series(pd.NA, index=dataframe.index, dtype=dtype)
        tables[table] = dataframe[columns]
    return tables
