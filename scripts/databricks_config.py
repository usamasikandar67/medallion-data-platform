import os
import sys

# Load env variables (reusing the new env_loader utility)
try:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
except NameError:
    sys.path.append("/Volumes/workspace/bronze/raw")
    sys.path.append("/Volumes/workspace/bronze/raw/scripts")
from utility.env_loader import load_env
load_env()

class DatabricksConfig:
    """
    Centralized configuration for Databricks Medallion Architecture.
    Uses environment variables for catalog and schema mapping to ensure
    deployment flexibility across Dev/Staging/Prod workspaces.
    """
    
    # Unity Catalog Settings
    CATALOG = os.getenv("DB_CATALOG", "workspace")
    SCHEMA_BRONZE = os.getenv("DB_SCHEMA_BRONZE", "bronze")
    SCHEMA_SILVER = os.getenv("DB_SCHEMA_SILVER", "silver")
    SCHEMA_GOLD = os.getenv("DB_SCHEMA_GOLD", "gold")
    SCHEMA_MEDALLION = os.getenv("DB_SCHEMA_MEDALLION", "medallion")

    # Table Names (Silver)
    TABLE_SILVER_CUSTOMERS = f"{CATALOG}.{SCHEMA_SILVER}.silver_customers"
    TABLE_SILVER_ORDERS = f"{CATALOG}.{SCHEMA_SILVER}.silver_orders"
    
    # Quarantine Tables
    TABLE_QUARANTINE_CUSTOMERS = f"{CATALOG}.{SCHEMA_SILVER}.quarantine_customers"
    TABLE_QUARANTINE_ORDERS = f"{CATALOG}.{SCHEMA_SILVER}.quarantine_orders"

    # Table Names (Gold)
    TABLE_GOLD_DIM_CUSTOMERS = f"{CATALOG}.{SCHEMA_GOLD}.dim_customers"
    TABLE_GOLD_FACT_ORDERS = f"{CATALOG}.{SCHEMA_GOLD}.fact_orders"
    TABLE_GOLD_AGG_DAILY_SALES = f"{CATALOG}.{SCHEMA_GOLD}.agg_daily_sales"
    TABLE_GOLD_AGG_CUSTOMER_GROWTH = f"{CATALOG}.{SCHEMA_GOLD}.agg_customer_growth"
    TABLE_GOLD_AGG_REVENUE_TRENDS = f"{CATALOG}.{SCHEMA_GOLD}.agg_revenue_trends"

    # Observability
    TABLE_AUDIT_LOG = f"{CATALOG}.{SCHEMA_MEDALLION}.pipeline_audit_log"

config = DatabricksConfig()
