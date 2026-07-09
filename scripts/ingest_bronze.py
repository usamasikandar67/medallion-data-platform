#!/usr/bin/env python3
import os
import sys
import glob
import json
import uuid
import shutil
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

CDC_BUFFER_DIR = os.getenv("CDC_BUFFER_DIR", "data/cdc_buffer")
BRONZE_DIR = "data/bronze"
CDC_ARCHIVE_DIR = "data/cdc_archive"

def ingest_bronze():
    # 1. Find all matching JSON batch files in the CDC buffer
    file_pattern = os.path.join(CDC_BUFFER_DIR, "cdc_batch_*.json")
    batch_files = glob.glob(file_pattern)
    
    # 2. Exit gracefully if no files are found
    if not batch_files:
        print("No new CDC batch files found to ingest in Bronze layer.")
        return 0
        
    print(f"Found {len(batch_files)} CDC files to ingest. Starting ingestion...")
    
    # 3. Read all files into a unified dataset
    all_records = []
    processed_files = []
    
    for file_path in batch_files:
        try:
            with open(file_path, 'r') as f:
                records = json.load(f)
                all_records.extend(records)
                processed_files.append(file_path)
        except Exception as e:
            print(f"Error reading file {file_path}: {e}. Skipping this file.")
            
    if not all_records:
        print("No records extracted from the CDC batch files.")
        return

    # 4. Generate batch metadata
    batch_id = str(uuid.uuid4())
    ingest_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    ingest_date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    # Convert records list to Pandas DataFrame
    df = pd.DataFrame(all_records)
    
    # Add Bronze ingestion metadata fields
    df["_bronze_ingested_at"] = ingest_time
    df["_bronze_batch_id"] = batch_id
    df["ingest_date"] = ingest_date_str
    
    # Schema specifications for entities
    customer_cols = [
        "_row_sequence_id", "entity_type", "_change_type", "_commit_timestamp",
        "customer_id", "first_name", "last_name", "email", "state", "created_at", "updated_at",
        "_bronze_ingested_at", "_bronze_batch_id"
    ]
    
    order_cols = [
        "_row_sequence_id", "entity_type", "_change_type", "_commit_timestamp",
        "order_id", "customer_id", "order_amount", "order_status", "created_at", "updated_at",
        "_bronze_ingested_at", "_bronze_batch_id"
    ]

    # 5. Group and write to Parquet by entity_type and ingest_date
    grouped = df.groupby(["entity_type", "ingest_date"])
    
    for (entity_type, ingest_date), group_df in grouped:
        # Select and conform schema columns for the target entity
        if entity_type == "customers":
            expected_cols = customer_cols
        elif entity_type == "orders":
            expected_cols = order_cols
        else:
            print(f"Unknown entity type encountered: {entity_type}. Skipping writing.")
            continue
            
        # Detect schema drift (new columns not in expected cols)
        incoming_cols = set(group_df.columns)
        expected_set = set(expected_cols)
        # Exclude internal grouping/partition columns
        drift_cols = incoming_cols - expected_set - {"ingest_date", "entity_type"}
        
        if drift_cols:
            print(f"[WARNING] Schema Drift: New columns detected in {entity_type}: {list(drift_cols)}")
            
        # Conforming target columns: expected columns first, then append evolved columns
        target_cols = [col for col in expected_cols if col in group_df.columns] + [col for col in drift_cols if col in group_df.columns]
        entity_df = group_df[target_cols].copy()
            
        # Target partition directory
        partition_dir = os.path.join(BRONZE_DIR, entity_type, f"ingest_date={ingest_date}")
        os.makedirs(partition_dir, exist_ok=True)
        
        # Target output parquet filename
        parquet_file_path = os.path.join(partition_dir, f"batch_{batch_id}.parquet")
        
        # Save Parquet using pyarrow engine
        try:
            entity_df.to_parquet(parquet_file_path, index=False, engine='pyarrow')
            print(f"Appended {len(entity_df)} {entity_type} records to partition ingest_date={ingest_date}")
        except Exception as e:
            print(f"Failed to write Parquet for {entity_type}: {e}")
            raise e
            
    # 6. Archive processed JSON files
    os.makedirs(CDC_ARCHIVE_DIR, exist_ok=True)
    for file_path in processed_files:
        dest_path = os.path.join(CDC_ARCHIVE_DIR, os.path.basename(file_path))
        try:
            shutil.move(file_path, dest_path)
        except Exception as e:
            print(f"Error archiving file {file_path} to {dest_path}: {e}")
            
    print(f"Bronze ingestion complete. Batch ID: {batch_id}. Processed {len(all_records)} total records from {len(processed_files)} batch files.")
    return len(all_records)

if __name__ == "__main__":
    import pipeline_logger
    run_id, start_time = pipeline_logger.log_start("Bronze Ingest")
    try:
        count = ingest_bronze()
        pipeline_logger.log_success(run_id, start_time, count)
    except Exception as e:
        pipeline_logger.log_failure(run_id, start_time, e)
        raise e
