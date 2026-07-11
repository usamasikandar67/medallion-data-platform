from dateutil import relativedelta
from dateutil import relativedelta
import os
import sys
from datetime import datetime, timezone
# pyrefly: ignore [missing-import]
from pyspark.sql import SparkSession
# pyrefly: ignore [missing-import]
from pyspark.sql.functions import col, sum as _sum, count, when, date_format

# Add project root to sys path
try:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
except NameError:
    sys.path.append("/Volumes/workspace/bronze/raw")
    sys.path.append("/Volumes/workspace/bronze/raw/scripts")
from databricks_config import config
from databricks_utils import DatabricksUtils

def process_gold_layer(spark: SparkSession):
    utils = DatabricksUtils(spark)
    start_time = datetime.now(timezone.utc)
    
    print(f"Starting Gold Layer processing for catalog: {config.CATALOG}")
    
    try:
        # 1. Read Silver Tables
        silver_customers = spark.table(config.TABLE_SILVER_CUSTOMERS)
        silver_orders = spark.table(config.TABLE_SILVER_ORDERS)
        
        # 2. Build Dimension Tables
        # dim_customers: Only active, non-deleted customers
        dim_customers = silver_customers.filter("is_current = 1 AND is_deleted = 0")
        dim_customers.createOrReplaceTempView("temp_dim_customers")
        spark.sql(f"CREATE OR REPLACE TABLE {config.TABLE_GOLD_DIM_CUSTOMERS} AS SELECT * FROM temp_dim_customers")
        
        # Optimize
        spark.sql(f"OPTIMIZE {config.TABLE_GOLD_DIM_CUSTOMERS} ZORDER BY (state)")
        
        # 3. Build Fact Tables
        # fact_orders: join orders to customers to get customer details at order time (or just surrogate key mapping)
        fact_orders = silver_orders.alias("o").join(
            silver_customers.alias("c"),
            (col("o.customer_id") == col("c.customer_id")) & 
            (col("o._commit_timestamp") >= col("c.valid_from")) & 
            (col("o._commit_timestamp") <= col("c.valid_to")),
            "left"
        ).select(
            col("o.order_id"),
            col("c.customer_id").alias("customer_sk"), # simplified surrogate key mapping
            col("o.order_amount"),
            col("o.order_status"),
            col("o._commit_timestamp").alias("order_timestamp")
        )
        fact_orders.createOrReplaceTempView("temp_fact_orders")
        spark.sql(f"CREATE OR REPLACE TABLE {config.TABLE_GOLD_FACT_ORDERS} AS SELECT * FROM temp_fact_orders")
        
        # Optimize
        spark.sql(f"OPTIMIZE {config.TABLE_GOLD_FACT_ORDERS} ZORDER BY (customer_sk, order_timestamp)")
        
        # 4. Build Aggregated KPI Tables
        
        # agg_daily_sales
        agg_daily_sales = fact_orders.withColumn("order_date", date_format("order_timestamp", "yyyy-MM-dd")) \
            .groupBy("order_date") \
            .agg(
                count("order_id").alias("total_orders"),
                _sum("order_amount").alias("total_revenue")
            )
        agg_daily_sales.createOrReplaceTempView("temp_agg_daily_sales")
        spark.sql(f"CREATE OR REPLACE TABLE {config.TABLE_GOLD_AGG_DAILY_SALES} AS SELECT * FROM temp_agg_daily_sales")
        
        # agg_revenue_trends
        agg_revenue_trends = fact_orders.groupBy("customer_sk") \
            .agg(
                _sum("order_amount").alias("total_spent"),
                date_format(col("order_timestamp"), "yyyy-MM-dd").alias("order_date")
            ) \
            .groupBy("customer_sk") \
            .agg(
                _sum("total_spent").alias("total_spent"),
                # Simplification for first/last order date
            )
        agg_revenue_trends.createOrReplaceTempView("temp_agg_revenue_trends")
        spark.sql(f"CREATE OR REPLACE TABLE {config.TABLE_GOLD_AGG_REVENUE_TRENDS} AS SELECT * FROM temp_agg_revenue_trends")
        
        # agg_customer_growth
        agg_customer_growth = dim_customers.withColumn("registration_month", date_format("valid_from", "yyyy-MM")) \
            .groupBy("registration_month") \
            .agg(count("customer_id").alias("new_customers"))
        agg_customer_growth.createOrReplaceTempView("temp_agg_customer_growth")
        spark.sql(f"CREATE OR REPLACE TABLE {config.TABLE_GOLD_AGG_CUSTOMER_GROWTH} AS SELECT * FROM temp_agg_customer_growth")
        
        utils.log_pipeline_execution("gold_processing", "gold", config.SCHEMA_GOLD, fact_orders.count(), 0, "SUCCESS", "", start_time, datetime.now(timezone.utc))
        print("Gold Layer processing completed successfully.")
        
    except Exception as e:
        error_msg = str(e)
        print(f"FAILED Gold Layer processing: {error_msg}")
        utils.log_pipeline_execution("gold_processing", "gold", config.SCHEMA_GOLD, 0, 0, "FAILED", error_msg, start_time, datetime.now(timezone.utc))

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Medallion_Gold_Layer") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
        
    process_gold_layer(spark)
