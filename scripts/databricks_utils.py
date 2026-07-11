import os
import sys
from datetime import datetime, timezone
# pyrefly: ignore [missing-import]
from pyspark.sql import SparkSession, DataFrame
# pyrefly: ignore [missing-import]
from pyspark.sql.functions import col, lit, current_timestamp, when, length
from databricks_config import config

class DatabricksUtils:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def setup_audit_table(self):
        """Creates the pipeline audit log table if it doesn't exist."""
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {config.TABLE_AUDIT_LOG} (
                log_id STRING,
                pipeline_name STRING,
                layer STRING,
                target_table STRING,
                records_processed BIGINT,
                records_quarantined BIGINT,
                status STRING,
                error_message STRING,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                duration_seconds DOUBLE
            )
            USING DELTA
        """)

    def log_pipeline_execution(self, pipeline_name: str, layer: str, target_table: str, 
                               records_processed: int, records_quarantined: int, 
                               status: str, error_message: str, 
                               start_time: datetime, end_time: datetime):
        """Logs pipeline execution metrics to the Medallion audit table."""
        import uuid
        
        self.setup_audit_table()
        
        duration = (end_time - start_time).total_seconds()
        
        log_df = self.spark.createDataFrame([{
            "log_id": str(uuid.uuid4()),
            "pipeline_name": pipeline_name,
            "layer": layer,
            "target_table": target_table,
            "records_processed": records_processed,
            "records_quarantined": records_quarantined,
            "status": status,
            "error_message": error_message,
            "start_time": start_time,
            "end_time": end_time,
            "duration_seconds": duration
        }])
        
        # Append to audit log
        log_df.write.format("delta").mode("append").saveAsTable(config.TABLE_AUDIT_LOG)
        print(f"[{status}] Logged execution for {target_table}. Processed: {records_processed}, Quarantined: {records_quarantined}")

    def apply_data_quality_rules(self, df: DataFrame, entity_type: str) -> tuple[DataFrame, DataFrame]:
        """
        Applies configurable data quality rules to a DataFrame.
        Returns a tuple: (clean_df, quarantine_df)
        """
        if entity_type == "customers":
            # Rule 1: customer_id must not be null
            # Rule 2: email must contain '@' and '.'
            # Rule 3: state must be 2 characters (if provided)
            
            dq_df = df.withColumn(
                "_dq_failed",
                when(col("customer_id").isNull(), lit(True))
                .when(col("email").isNotNull() & (~col("email").contains("@") | ~col("email").contains(".")), lit(True))
                .when(col("state").isNotNull() & (length(col("state")) != 2), lit(True))
                .otherwise(lit(False))
            )
            
            dq_df = dq_df.withColumn(
                "_quarantine_reason",
                when(col("customer_id").isNull(), lit("NULL_PK"))
                .when(col("email").isNotNull() & (~col("email").contains("@") | ~col("email").contains(".")), lit("INVALID_EMAIL"))
                .when(col("state").isNotNull() & (length(col("state")) != 2), lit("INVALID_STATE_CODE"))
                .otherwise(lit(None))
            )
            
        elif entity_type == "orders":
            # Rule 1: order_id must not be null
            # Rule 2: order_amount must be >= 0
            
            dq_df = df.withColumn(
                "_dq_failed",
                when(col("order_id").isNull(), lit(True))
                .when(col("order_amount") < 0, lit(True))
                .otherwise(lit(False))
            )
            
            dq_df = dq_df.withColumn(
                "_quarantine_reason",
                when(col("order_id").isNull(), lit("NULL_PK"))
                .when(col("order_amount") < 0, lit("NEGATIVE_AMOUNT"))
                .otherwise(lit(None))
            )
        else:
            # Pass-through for unknown entities
            return df, self.spark.createDataFrame([], df.schema)

        # Split into clean and quarantine
        clean_df = dq_df.filter(col("_dq_failed") == False).drop("_dq_failed", "_quarantine_reason")
        
        quarantine_df = dq_df.filter(col("_dq_failed") == True)
        # Add quarantine timestamp
        quarantine_df = quarantine_df.withColumn("_quarantined_at", current_timestamp())
        
        return clean_df, quarantine_df
