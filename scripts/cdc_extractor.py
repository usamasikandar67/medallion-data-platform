#!/usr/bin/env python3
import os
import sys
import json
import sqlite3
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

DATABASE_PATH = os.getenv("DATABASE_PATH", "data/source_oltp.db")
CDC_BUFFER_DIR = os.getenv("CDC_BUFFER_DIR", "data/cdc_buffer")
CDC_STATE_FILE = os.getenv("CDC_STATE_FILE", "data/cdc_state.json")

os.makedirs(CDC_BUFFER_DIR, exist_ok=True)

def read_state():
    if os.path.exists(CDC_STATE_FILE):
        try:
            with open(CDC_STATE_FILE, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError:
            pass
    return {
        "last_processed_id": 0,
        "last_processed_timestamp": "1970-01-01 00:00:00.000"
    }

def write_state(last_id, last_ts):
    state = {
        "last_processed_id": last_id,
        "last_processed_timestamp": last_ts
    }
    with open(CDC_STATE_FILE, 'w') as f:
        json.dump(state, f, indent=4)

def extract_cdc():
    state = read_state()
    last_id = state.get("last_processed_id", 0)
    
    if not os.path.exists(DATABASE_PATH):
        print(f"Database file not found at {DATABASE_PATH}. No extraction performed.")
        return

    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    try:
        # Check if the log table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='cdc_event_log';")
        if not cursor.fetchone():
            print("cdc_event_log table does not exist yet. Running simulator first is required.")
            conn.close()
            return
            
        cursor.execute(
            "SELECT * FROM cdc_event_log WHERE _row_sequence_id > ? ORDER BY _row_sequence_id ASC;",
            (last_id,)
        )
        rows = cursor.fetchall()
        
        if not rows:
            print("No new change data captured since last extraction.")
            conn.close()
            return
            
        extracted_records = []
        max_id = last_id
        max_ts = state.get("last_processed_timestamp", "1970-01-01 00:00:00.000")
        
        for row in rows:
            record = {}
            entity_type = row["entity_type"]
            
            # Map common metadata fields
            record["_row_sequence_id"] = row["_row_sequence_id"]
            record["entity_type"] = row["entity_type"]
            record["_change_type"] = row["_change_type"]
            record["_commit_timestamp"] = row["_commit_timestamp"]
            
            if entity_type == "customers":
                for col in ["customer_id", "first_name", "last_name", "email", "state", "created_at", "updated_at"]:
                    record[col] = row[col]
            elif entity_type == "orders":
                for col in ["order_id", "customer_id", "order_amount", "order_status", "created_at", "updated_at"]:
                    record[col] = row[col]
                    
            extracted_records.append(record)
            
            # Track high-water marks
            if row["_row_sequence_id"] > max_id:
                max_id = row["_row_sequence_id"]
            if row["_commit_timestamp"] > max_ts:
                max_ts = row["_commit_timestamp"]
                
        # Write to JSON file in cdc_buffer
        file_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        output_file_name = f"cdc_batch_{file_ts}.json"
        output_file_path = os.path.join(CDC_BUFFER_DIR, output_file_name)
        
        with open(output_file_path, 'w') as f:
            json.dump(extracted_records, f, indent=4)
            
        # MOCK KAFKA STREAMING: Append to local streaming topic file
        kafka_topic_dir = "data/kafka_topics"
        os.makedirs(kafka_topic_dir, exist_ok=True)
        kafka_log_path = os.path.join(kafka_topic_dir, "cdc_stream.log")
        with open(kafka_log_path, 'a') as kf:
            for rec in extracted_records:
                kf.write(json.dumps(rec) + "\n")
        print(f"  Mock Stream: Appended {len(extracted_records)} events to Kafka topic log 'cdc_stream.log'")
            
        # Update high-water mark state file
        write_state(max_id, max_ts)
        
        print(f"Successfully extracted {len(extracted_records)} mutations into CDC feed file:")
        print(f"  Path: {output_file_path}")
        print(f"  Updated High Water Mark: ID={max_id}, Timestamp={max_ts}")
        
    except Exception as e:
        print(f"Error during CDC extraction: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    extract_cdc()
