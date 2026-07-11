#!/usr/bin/env python3
import os
import uuid
import sqlite3
from datetime import datetime, timezone

try:
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    BASE_DIR = "/Volumes/workspace/bronze/raw"

IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ or "/Volumes/" in BASE_DIR or "DB_CLUSTER_ID" in os.environ

WAREHOUSE_DB = os.path.join(BASE_DIR, "data/warehouse.db")
DDL_FILE = os.path.join(BASE_DIR, "scripts/create_monitoring_schema.sql")

def initialize_monitoring_schema():
    if IS_DATABRICKS:
        return
    if not os.path.exists(DDL_FILE):
        return
    os.makedirs(os.path.dirname(WAREHOUSE_DB), exist_ok=True)
    try:
        conn = sqlite3.connect(WAREHOUSE_DB)
        with open(DDL_FILE, 'r') as f:
            ddl_sql = f.read()
        conn.executescript(ddl_sql)
        conn.commit()
        conn.close()
    except sqlite3.OperationalError:
        pass

# Initialize dynamically on import
initialize_monitoring_schema()

def log_start(pipeline_name):
    run_id = str(uuid.uuid4())
    start_time = datetime.now(timezone.utc).isoformat()
    if IS_DATABRICKS:
        return run_id, start_time
    
    conn = sqlite3.connect(WAREHOUSE_DB)
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO monitor_pipeline_runs (run_id, pipeline_name, start_time, status)
        VALUES (?, ?, ?, 'RUNNING');
        """,
        (run_id, pipeline_name, start_time)
    )
    conn.commit()
    conn.close()
    
    return run_id, start_time

def log_success(run_id, start_time_iso, records_processed=0):
    if IS_DATABRICKS:
        return
    end_time = datetime.now(timezone.utc).isoformat()
    start_dt = datetime.fromisoformat(start_time_iso)
    end_dt = datetime.fromisoformat(end_time)
    duration = (end_dt - start_dt).total_seconds()
    
    conn = sqlite3.connect(WAREHOUSE_DB)
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE monitor_pipeline_runs
        SET end_time = ?, duration_seconds = ?, status = 'SUCCESS', records_processed = ?
        WHERE run_id = ?;
        """,
        (end_time, duration, records_processed, run_id)
    )
    conn.commit()
    conn.close()

def log_failure(run_id, start_time_iso, error_message):
    if IS_DATABRICKS:
        return
    end_time = datetime.now(timezone.utc).isoformat()
    start_dt = datetime.fromisoformat(start_time_iso)
    end_dt = datetime.fromisoformat(end_time)
    duration = (end_dt - start_dt).total_seconds()
    
    conn = sqlite3.connect(WAREHOUSE_DB)
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE monitor_pipeline_runs
        SET end_time = ?, duration_seconds = ?, status = 'FAILED', error_message = ?
        WHERE run_id = ?;
        """,
        (end_time, duration, str(error_message), run_id)
    )
    conn.commit()
    conn.close()
