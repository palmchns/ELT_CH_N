"""Runtime defaults and source table mappings."""
import os
from datetime import datetime

OBS_DIR = os.getenv("OBS_DIR", "/app/obs_data" if os.getenv("IN_DOCKER") else "./obs_data")
TODAY = datetime.now()
YEAR, MONTH, DAY = TODAY.strftime("%Y"), TODAY.strftime("%m"), TODAY.strftime("%d")
RUN_HHMMSS = "000000"
ZONE_CODES = {"ingress": "i", "landing": "l", "staging": "s", "integration": "I"}
SOURCES = {
    "chinook": "https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_Sqlite.sqlite",
    "northwind": "https://github.com/jpwhite3/northwind-SQLite3/raw/main/dist/northwind.db",
}

# Canonical table keys are used only by the landing/staging/integration layers.
# Ingress keeps the exact table names reported by each source database.
INGRESS_TABLES = {
    "chinook": {
        "customer": "Customer",
        "employee": "Employee",
        "track": "Track",
        "album": "Album",
        "artist": "Artist",
        "genre": "Genre",
        "invoice": "Invoice",
        "invoiceline": "InvoiceLine",
    },
    "northwind": {
        "customers": "Customers",
        "employees": "Employees",
        "products": "Products",
        "categories": "Categories",
        "suppliers": "Suppliers",
        "orders": "Orders",
        "order_details": "Order Details",
    },
}
