#!/usr/bin/env python3
import os
import glob
import sqlite3
import pandas as pd

WAREHOUSE_DB = "data/warehouse.db"
BRONZE_DIR = "data/bronze"

def load_bronze_to_sqlite():
    print("Loading Bronze Parquet files into SQLite raw source tables for dbt...")
    os.makedirs(os.path.dirname(WAREHOUSE_DB), exist_ok=True)
    conn = sqlite3.connect(WAREHOUSE_DB)
    
    # 1. Load Customers
    customer_files = glob.glob(os.path.join(BRONZE_DIR, "customers", "ingest_date=*", "*.parquet"))
    if customer_files:
        cust_df = pd.concat([pd.read_parquet(f) for f in customer_files], ignore_index=True)
        cust_df.to_sql("raw_customers", conn, if_exists="replace", index=False)
        print(f"Loaded {len(cust_df)} customer rows to SQLite table raw_customers.")
    else:
        print("No Bronze customer parquet files found.")
        
    # 2. Load Orders
    order_files = glob.glob(os.path.join(BRONZE_DIR, "orders", "ingest_date=*", "*.parquet"))
    if order_files:
        order_df = pd.concat([pd.read_parquet(f) for f in order_files], ignore_index=True)
        order_df.to_sql("raw_orders", conn, if_exists="replace", index=False)
        print(f"Loaded {len(order_df)} order rows to SQLite table raw_orders.")
    else:
        print("No Bronze order parquet files found.")
        
    conn.commit()
    conn.close()
    print("Bronze loading complete.")

if __name__ == "__main__":
    load_bronze_to_sqlite()
