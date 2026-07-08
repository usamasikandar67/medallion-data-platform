#!/usr/bin/env python3
import os
import sys
import glob
import json
import hashlib
import sqlite3
import pandas as pd
from datetime import datetime, timezone

# Simple .env parser to avoid external dependencies
def load_env():
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
    if os.path.exists(env_path):
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, val = line.split('=', 1)
                    os.environ[key.strip()] = val.strip()

load_env()

WAREHOUSE_DB = "data/warehouse.db"
SILVER_STATE_FILE = "data/silver_state.json"
QUARANTINE_DIR = "data/quarantine"
BRONZE_DIR = "data/bronze"
DDL_FILE = "scripts/create_silver_tables.sql"

os.makedirs(QUARANTINE_DIR, exist_ok=True)

def read_processed_batches():
    if os.path.exists(SILVER_STATE_FILE):
        try:
            with open(SILVER_STATE_FILE, 'r') as f:
                return set(json.load(f))
        except json.JSONDecodeError:
            pass
    return set()

def write_processed_batches(batch_set):
    with open(SILVER_STATE_FILE, 'w') as f:
        json.dump(list(batch_set), f, indent=4)

def initialize_database():
    print(f"Initializing warehouse database schema using {DDL_FILE}...")
    if not os.path.exists(DDL_FILE):
        print(f"Error: DDL script not found at {DDL_FILE}")
        sys.exit(1)
        
    os.makedirs(os.path.dirname(WAREHOUSE_DB), exist_ok=True)
    conn = sqlite3.connect(WAREHOUSE_DB)
    
    with open(DDL_FILE, 'r') as f:
        ddl_sql = f.read()
        
    conn.executescript(ddl_sql)
    conn.commit()
    conn.close()

def generate_surrogate_key(customer_id, valid_from):
    raw_str = f"{customer_id}_{valid_from}"
    return hashlib.md5(raw_str.encode('utf-8')).hexdigest()

def clean_string(val):
    if pd.isna(val) or val is None:
        return ""
    return str(val).strip()

def process_batch(batch_id):
    print(f"Processing incremental Bronze Batch: {batch_id}")
    
    # 1. Locate files for this batch
    customer_files = glob.glob(os.path.join(BRONZE_DIR, "customers", "ingest_date=*", f"batch_{batch_id}.parquet"))
    order_files = glob.glob(os.path.join(BRONZE_DIR, "orders", "ingest_date=*", f"batch_{batch_id}.parquet"))
    
    quarantine_records = []
    
    conn = sqlite3.connect(WAREHOUSE_DB)
    conn.execute("PRAGMA foreign_keys=OFF;") # We do not enforce cross-entity foreign keys at Silver
    cursor = conn.cursor()
    
    try:
        # Start transactional unit
        cursor.execute("BEGIN TRANSACTION;")
        
        # -------------------------------------------------------------
        # A. PROCESS CUSTOMERS
        # -------------------------------------------------------------
        if customer_files:
            # Union all matching customer files (typically just 1)
            cust_dfs = [pd.read_parquet(f) for f in customer_files]
            cust_df = pd.concat(cust_dfs, ignore_index=True)
            
            # Sort chronologically by sequence id and timestamp
            cust_df = cust_df.sort_values(by=["_commit_timestamp", "_row_sequence_id"], ascending=True)
            
            for idx, row in cust_df.iterrows():
                customer_id = clean_string(row.get("customer_id"))
                change_type = clean_string(row.get("_change_type"))
                commit_ts = clean_string(row.get("_commit_timestamp"))
                
                # Check for NULL key -> quarantine
                if not customer_id:
                    quarantine_records.append({
                        "entity_type": "customers",
                        "reason": "Missing customer_id",
                        "record": row.to_dict(),
                        "quarantined_at": datetime.now(timezone.utc).isoformat()
                    })
                    continue
                    
                # Ignore UPDATE_BEFORE as UPDATE_AFTER contains the net changes
                if change_type == "UPDATE_BEFORE":
                    continue
                    
                if change_type == "DELETE":
                    # Close the current active row
                    cursor.execute(
                        """
                        UPDATE silver_customers 
                        SET valid_to = ?, is_current = 0, is_deleted = 1, _bronze_batch_id = ?
                        WHERE customer_id = ? AND is_current = 1;
                        """,
                        (commit_ts, batch_id, customer_id)
                    )
                elif change_type in ["INSERT", "UPDATE_AFTER"]:
                    first_name = clean_string(row.get("first_name"))
                    last_name = clean_string(row.get("last_name"))
                    email = clean_string(row.get("email"))
                    state = clean_string(row.get("state")).upper()
                    created_at = clean_string(row.get("created_at"))
                    updated_at = clean_string(row.get("updated_at"))
                    
                    # Check if there is an active version
                    cursor.execute(
                        "SELECT valid_from FROM silver_customers WHERE customer_id = ? AND is_current = 1;",
                        (customer_id,)
                    )
                    active_row = cursor.fetchone()
                    
                    if active_row:
                        active_valid_from = active_row[0]
                        # Safe handling of out-of-order events
                        if commit_ts <= active_valid_from:
                            print(f"Warning: Out-of-order event for customer {customer_id}. Incoming timestamp {commit_ts} <= active row validity {active_valid_from}. Skipping.")
                            continue
                            
                        # Close existing version
                        cursor.execute(
                            """
                            UPDATE silver_customers 
                            SET valid_to = ?, is_current = 0 
                            WHERE customer_id = ? AND is_current = 1;
                            """,
                            (commit_ts, customer_id)
                        )
                        
                    # Insert the new version
                    sk = generate_surrogate_key(customer_id, commit_ts)
                    cursor.execute(
                        """
                        INSERT INTO silver_customers (
                            customer_sk, customer_id, first_name, last_name, email, state, 
                            valid_from, valid_to, is_current, is_deleted, _bronze_batch_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, '9999-12-31 23:59:59', 1, 0, ?);
                        """,
                        (sk, customer_id, first_name, last_name, email, state, commit_ts, batch_id)
                    )
                    
        # -------------------------------------------------------------
        # B. PROCESS ORDERS
        # -------------------------------------------------------------
        if order_files:
            order_dfs = [pd.read_parquet(f) for f in order_files]
            order_df = pd.concat(order_dfs, ignore_index=True)
            
            # Sort chronologically
            order_df = order_df.sort_values(by=["_commit_timestamp", "_row_sequence_id"], ascending=True)
            
            for idx, row in order_df.iterrows():
                order_id = clean_string(row.get("order_id"))
                customer_id = clean_string(row.get("customer_id"))
                change_type = clean_string(row.get("_change_type"))
                
                # Check for NULL primary key or FK -> quarantine
                if not order_id or not customer_id:
                    quarantine_records.append({
                        "entity_type": "orders",
                        "reason": f"Missing {'order_id' if not order_id else 'customer_id'}",
                        "record": row.to_dict(),
                        "quarantined_at": datetime.now(timezone.utc).isoformat()
                    })
                    continue
                    
                # Parse amount and handle potential format error -> quarantine
                try:
                    amount_val = row.get("order_amount")
                    if amount_val is None or pd.isna(amount_val):
                        raise ValueError("Null order amount")
                    amount = float(amount_val)
                except Exception as e:
                    quarantine_records.append({
                        "entity_type": "orders",
                        "reason": f"Invalid order_amount: {e}",
                        "record": row.to_dict(),
                        "quarantined_at": datetime.now(timezone.utc).isoformat()
                    })
                    continue
                    
                if change_type == "DELETE":
                    cursor.execute("DELETE FROM silver_orders WHERE order_id = ?;", (order_id,))
                else:
                    status = clean_string(row.get("order_status"))
                    created_at = clean_string(row.get("created_at"))
                    updated_at = clean_string(row.get("updated_at"))
                    
                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO silver_orders (
                            order_id, customer_id, order_amount, order_status, created_at, updated_at, _bronze_batch_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?);
                        """,
                        (order_id, customer_id, amount, status, created_at, updated_at, batch_id)
                    )
                    
        # Write quarantine records if any were captured
        if quarantine_records:
            ts_suffix = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
            quarantine_file = os.path.join(QUARANTINE_DIR, f"quarantine_{batch_id}_{ts_suffix}.json")
            with open(quarantine_file, 'w') as f:
                json.dump(quarantine_records, f, indent=4)
            print(f"Quarantined {len(quarantine_records)} invalid rows to {quarantine_file}")

        # Commit transaction unit
        conn.commit()
        print(f"Successfully committed Batch {batch_id} to Silver conformed layer.")
        
    except Exception as e:
        conn.rollback()
        print(f"Error during merge execution of batch {batch_id}: {e}. Transaction rolled back.")
        raise e
    finally:
        conn.close()

def main():
    initialize_database()
    
    # Read processed list
    processed_batches = read_processed_batches()
    
    # Scan all parquet files in Bronze folder to discover existing batches
    parquet_paths = glob.glob(os.path.join(BRONZE_DIR, "**", "*.parquet"), recursive=True)
    all_batches = set()
    
    for path in parquet_paths:
        filename = os.path.basename(path)
        # Format is batch_{uuid}.parquet
        if filename.startswith("batch_") and filename.endswith(".parquet"):
            batch_id = filename[6:-8]
            all_batches.add(batch_id)
            
    unprocessed_batches = all_batches - processed_batches
    
    if not unprocessed_batches:
        print("No new Bronze batches found. Silver conformed tables are up to date.")
        return
        
    print(f"Found {len(unprocessed_batches)} unprocessed batches. Starting loading...")
    
    # Process each batch sequentially
    for batch_id in sorted(list(unprocessed_batches)):
        process_batch(batch_id)
        processed_batches.add(batch_id)
        write_processed_batches(processed_batches)
        
    print("Silver conformed layer execution completed successfully.")

if __name__ == "__main__":
    import pipeline_logger
    run_id, start_time = pipeline_logger.log_start("Silver Merge")
    try:
        main()
        pipeline_logger.log_success(run_id, start_time, 0)
    except Exception as e:
        pipeline_logger.log_failure(run_id, start_time, e)
        raise e
