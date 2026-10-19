"""Table-owned source transformations and Prefect staging flows."""
from . import flow_dim_customer, flow_dim_date, flow_dim_employee
from . import flow_dim_location, flow_dim_product, flow_fact_order

MODELS = {
    "dim_date": flow_dim_date,
    "dim_location": flow_dim_location,
    "dim_customer": flow_dim_customer,
    "dim_employee": flow_dim_employee,
    "dim_product": flow_dim_product,
    "fact_order": flow_fact_order,
}
MODEL_FLOWS = {table: getattr(module, f"flow_{table}") for table, module in MODELS.items()}
