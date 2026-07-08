#!/usr/bin/env python3
import os
import sqlite3

WAREHOUSE_DB = "data/warehouse.db"

def load_gold_dimensions(cursor):
    print("Loading dim_customers from silver_customers...")
    
    # 1. Insert/Replace the default "Unknown Customer" row
    cursor.execute(
        """
        INSERT OR REPLACE INTO dim_customers (
            customer_sk, customer_id, first_name, last_name, email, state, valid_from, valid_to, is_current
        ) VALUES (
            '-1', 'unknown', 'Unknown', 'Customer', 'unknown@example.com', 'UNKNOWN', 
            '1970-01-01 00:00:00.000', '9999-12-31 23:59:59.000', 1
        );
        """
    )
    
    # 2. Copy conformed customer histories from silver conformed tables
    cursor.execute(
        """
        INSERT OR REPLACE INTO dim_customers (
            customer_sk, customer_id, first_name, last_name, email, state, valid_from, valid_to, is_current
        )
        SELECT 
            customer_sk, customer_id, first_name, last_name, email, state, valid_from, valid_to, is_current
        FROM silver_customers
        WHERE is_deleted = 0; -- Only load active dimensions, soft-deletes are handled by validity expiration
        """
    )

def load_gold_facts(cursor):
    print("Loading fact_orders from silver_orders joined with dim_customers (SCD2)...")
    
    # 3. Perform point-in-time join using string timestamps comparison (chronological sorting order is preserved in ISO format)
    # We resolve date_key by formatting creation dates as YYYYMMDD.
    cursor.execute(
        """
        INSERT OR REPLACE INTO fact_orders (
            order_id, customer_sk, date_key, order_amount, order_status, created_at
        )
        SELECT
            o.order_id,
            COALESCE(c.customer_sk, '-1') as customer_sk,
            CAST(STRFTIME('%Y%m%d', o.created_at) as INTEGER) as date_key,
            o.order_amount,
            o.order_status,
            o.created_at
        FROM silver_orders o
        LEFT JOIN dim_customers c ON
            o.customer_id = c.customer_id
            AND o.created_at >= c.valid_from
            AND o.created_at < c.valid_to;
        """
    )

def main():
    if not os.path.exists(WAREHOUSE_DB):
        print(f"Error: Warehouse database not found at {WAREHOUSE_DB}. Run Silver loader first.")
        return

    conn = sqlite3.connect(WAREHOUSE_DB)
    cursor = conn.cursor()
    
    try:
        cursor.execute("BEGIN TRANSACTION;")
        
        # Load dimensions
        load_gold_dimensions(cursor)
        
        # Load facts
        load_gold_facts(cursor)
        
        conn.commit()
        print("Gold analytical layer loaded successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Error during Gold layer loading: {e}. Transaction rolled back.")
        raise e
    finally:
        conn.close()

if __name__ == "__main__":
    import pipeline_logger
    run_id, start_time = pipeline_logger.log_start("Gold Load")
    try:
        main()
        pipeline_logger.log_success(run_id, start_time, 0)
    except Exception as e:
        pipeline_logger.log_failure(run_id, start_time, e)
        raise e
