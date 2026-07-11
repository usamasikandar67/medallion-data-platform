import os
import sys
from datetime import datetime, timezone
# pyrefly: ignore [missing-import]
from pyspark.sql import SparkSession
# pyrefly: ignore [missing-import]
from pyspark.sql.functions import col, lit, current_timestamp
# pyrefly: ignore [missing-import]
from delta.tables import DeltaTable

# Add project root to sys path
try:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
except NameError:
    sys.path.append("/Volumes/workspace/bronze/raw")
    sys.path.append("/Volumes/workspace/bronze/raw/scripts")
from databricks_config import config
from databricks_utils import DatabricksUtils

def process_silver_layer(spark: SparkSession):
    utils = DatabricksUtils(spark)
    start_time = datetime.now(timezone.utc)
    
    print(f"Starting Silver Layer processing for catalog: {config.CATALOG}")
    
    # 1. Programmatic Discovery of Bronze Tables
    try:
        tables = spark.catalog.listTables(f"{config.CATALOG}.{config.SCHEMA_BRONZE}")
        bronze_table_names = [t.name for t in tables if not t.isTemporary]
        print(f"Discovered Bronze tables: {bronze_table_names}")
    except Exception as e:
        print(f"Error discovering tables: {e}. Defaulting to explicit lists.")
        bronze_table_names = ["customers", "orders"] # Fallback

    # 2. Process each Bronze table
    for table_name in bronze_table_names:
        print(f"Processing Bronze table: {table_name}")
        
        try:
            # Read Bronze table
            bronze_df = spark.table(f"{config.CATALOG}.{config.SCHEMA_BRONZE}.{table_name}")
            
            # Identify purely new CDC records (Optional: In a real pipeline, we'd use Structured Streaming or watermarks)
            # For batch, we'll process all records and rely on MERGE idempotency.
            
            # Apply Data Quality Rules
            clean_df, quarantine_df = utils.apply_data_quality_rules(bronze_df, table_name)
            
            # Handle Quarantined Records
            quarantine_count = quarantine_df.count()
            if quarantine_count > 0:
                quarantine_target = f"{config.CATALOG}.{config.SCHEMA_SILVER}.quarantine_{table_name}"
                quarantine_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(quarantine_target)
                print(f"Quarantined {quarantine_count} records to {quarantine_target}")
                
            clean_count = clean_df.count()
            if clean_count == 0:
                print(f"No valid records to process for {table_name}.")
                continue
                
            # Delta MERGE logic based on entity type
            target_table_name = f"{config.CATALOG}.{config.SCHEMA_SILVER}.silver_{table_name}"
            
            # Check if target table exists using SQL which is 100% reliable
            try:
                table_exists = spark.sql(f"SHOW TABLES IN {config.CATALOG}.{config.SCHEMA_SILVER} LIKE 'silver_{table_name}'").count() > 0
            except:
                table_exists = False
                
            if not table_exists:
                print(f"Target table {target_table_name} does not exist. Creating it...")
                # Add necessary Silver metadata columns for SCD2 / processing
                init_df = clean_df.withColumn("_silver_processed_at", current_timestamp())
                
                if table_name == "customers":
                    init_df = init_df.withColumn("is_current", lit(1)) \
                                     .withColumn("valid_from", col("_commit_timestamp")) \
                                     .withColumn("valid_to", lit("9999-12-31 23:59:59")) \
                                     .withColumn("is_deleted", lit(0))
                
                init_df.createOrReplaceTempView("temp_init_silver")
                spark.sql(f"CREATE TABLE IF NOT EXISTS {target_table_name} AS SELECT * FROM temp_init_silver")
                utils.log_pipeline_execution("silver_processing", "silver", target_table_name, clean_count, quarantine_count, "SUCCESS", "", start_time, datetime.now(timezone.utc))
                continue
                
            # If table exists, perform MERGE
            silver_table = DeltaTable.forName(spark, target_table_name)
            
            if table_name == "orders":
                # Standard Upsert for Orders
                (silver_table.alias("target")
                    .merge(clean_df.alias("source"), "target.order_id = source.order_id")
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute())
                    
            elif table_name == "customers":
                # SCD Type 2 for Customers
                # 1. Identify records that will cause an update (existing active records where data changed)
                updates_df = clean_df.alias("updates") \
                    .join(spark.table(target_table_name).alias("target"), "customer_id") \
                    .where("target.is_current = 1 AND updates._change_type != 'DELETE'") \
                    .selectExpr("NULL as mergeKey", "updates.*")
                    
                # 2. Union with original clean records for inserts
                staged_updates = clean_df.withColumn("mergeKey", col("customer_id")).unionByName(updates_df, allowMissingColumns=True)
                
                # 3. Execute MERGE
                (silver_table.alias("target")
                    .merge(staged_updates.alias("source"), "target.customer_id = source.mergeKey")
                    # Retire old records
                    .whenMatchedUpdate(
                        condition="target.is_current = 1 AND source._change_type != 'DELETE'",
                        set={"is_current": lit(0), "valid_to": "source._commit_timestamp"}
                    )
                    # Mark soft-deleted
                    .whenMatchedUpdate(
                        condition="target.is_current = 1 AND source._change_type == 'DELETE'",
                        set={"is_current": lit(0), "is_deleted": lit(1), "valid_to": "source._commit_timestamp"}
                    )
                    # Insert new records (both brand new and the new version of updated records)
                    .whenNotMatchedInsert(
                        condition="source._change_type != 'DELETE'",
                        values={
                            "customer_id": "source.customer_id",
                            "first_name": "source.first_name",
                            "last_name": "source.last_name",
                            "email": "source.email",
                            "state": "source.state",
                            "_commit_timestamp": "source._commit_timestamp",
                            "is_current": lit(1),
                            "valid_from": "source._commit_timestamp",
                            "valid_to": lit("9999-12-31 23:59:59"),
                            "is_deleted": lit(0),
                            "_silver_processed_at": current_timestamp()
                        }
                    )
                    .execute())
                    
            utils.log_pipeline_execution("silver_processing", "silver", target_table_name, clean_count, quarantine_count, "SUCCESS", "", start_time, datetime.now(timezone.utc))
            
        except Exception as e:
            error_msg = str(e)
            print(f"FAILED processing {table_name}: {error_msg}")
            utils.log_pipeline_execution("silver_processing", "silver", f"{config.CATALOG}.{config.SCHEMA_SILVER}.silver_{table_name}", 0, 0, "FAILED", error_msg, start_time, datetime.now(timezone.utc))

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Medallion_Silver_Layer") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
        
    process_silver_layer(spark)
