import os
import sys
# pyrefly: ignore [missing-import]
import pytest
from datetime import datetime
# pyrefly: ignore [missing-import]
from pyspark.sql import SparkSession
# pyrefly: ignore [missing-import]
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Add project root and scripts directory to sys path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)
sys.path.append(os.path.join(project_root, "scripts"))
from scripts.databricks_utils import DatabricksUtils

@pytest.fixture(scope="session")
def spark():
    """Create a local Spark session for testing."""
    return SparkSession.builder \
        .appName("Databricks_PySpark_Tests") \
        .master("local[2]") \
        .getOrCreate()

def test_apply_data_quality_rules_customers(spark):
    """Test that customer data quality rules correctly quarantine invalid records."""
    utils = DatabricksUtils(spark)
    
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("email", StringType(), True),
        StructField("state", StringType(), True)
    ])
    
    data = [
        ("uuid-1", "valid@example.com", "NY"),   # Valid
        (None, "nullpk@example.com", "CA"),      # Invalid: NULL PK
        ("uuid-2", "invalid_email.com", "TX"),   # Invalid: Bad email
        ("uuid-3", "valid2@example.com", "CAL")  # Invalid: State code > 2 chars
    ]
    
    df = spark.createDataFrame(data, schema)
    
    clean_df, quarantine_df = utils.apply_data_quality_rules(df, "customers")
    
    clean_results = clean_df.collect()
    quarantine_results = quarantine_df.collect()
    
    assert len(clean_results) == 1
    assert clean_results[0].customer_id == "uuid-1"
    
    assert len(quarantine_results) == 3
    
    quarantine_reasons = {row.customer_id: row._quarantine_reason for row in quarantine_results}
    assert quarantine_reasons[None] == "NULL_PK"
    assert quarantine_reasons["uuid-2"] == "INVALID_EMAIL"
    assert quarantine_reasons["uuid-3"] == "INVALID_STATE_CODE"

def test_apply_data_quality_rules_orders(spark):
    """Test that order data quality rules correctly quarantine negative amounts."""
    utils = DatabricksUtils(spark)
    
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("order_amount", DoubleType(), True)
    ])
    
    data = [
        ("order-1", 100.50),  # Valid
        ("order-2", -50.00),  # Invalid: Negative amount
        (None, 20.00)         # Invalid: NULL PK
    ]
    
    df = spark.createDataFrame(data, schema)
    clean_df, quarantine_df = utils.apply_data_quality_rules(df, "orders")
    
    assert clean_df.count() == 1
    assert quarantine_df.count() == 2
