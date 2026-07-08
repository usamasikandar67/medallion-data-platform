#!/usr/bin/env python3
import os
import uuid
import sqlite3
from datetime import datetime, timezone

WAREHOUSE_DB = "data/warehouse.db"
DDL_FILE = "scripts/create_monitoring_schema.sql"

def initialize_monitoring_schema():
    if not os.path.exists(DDL_FILE):
        return
    conn = sqlite3.connect(WAREHOUSE_DB)
    with open(DDL_FILE, 'r') as f:
        ddl_sql = f.read()
    conn.executescript(ddl_sql)
    conn.commit()
    conn.close()

# Initialize dynamically on import
initialize_monitoring_schema()

def log_start(pipeline_name):
    run_id = str(uuid.uuid4())
    start_time = datetime.now(timezone.utc).isoformat()
    
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
