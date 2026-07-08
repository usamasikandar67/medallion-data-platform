#!/usr/bin/env python3
import os
import sqlite3
from datetime import datetime, timedelta

WAREHOUSE_DB = "data/warehouse.db"
DDL_FILE = "scripts/create_gold_tables.sql"

def initialize_gold_schema():
    print(f"Initializing Gold schema using {DDL_FILE}...")
    if not os.path.exists(DDL_FILE):
        print(f"Error: DDL script not found at {DDL_FILE}")
        return False
        
    os.makedirs(os.path.dirname(WAREHOUSE_DB), exist_ok=True)
    conn = sqlite3.connect(WAREHOUSE_DB)
    with open(DDL_FILE, 'r') as f:
        ddl_sql = f.read()
    conn.executescript(ddl_sql)
    conn.commit()
    conn.close()
    return True

def generate_dim_date():
    if not initialize_gold_schema():
        return
        
    print("Generating static Date Dimension records (2025-01-01 to 2030-12-31)...")
    conn = sqlite3.connect(WAREHOUSE_DB)
    cursor = conn.cursor()
    
    start_date = datetime(2025, 1, 1)
    end_date = datetime(2030, 12, 31)
    current_date = start_date
    
    date_records = []
    
    while current_date <= end_date:
        date_key = int(current_date.strftime("%Y%m%d"))
        calendar_date = current_date.strftime("%Y-%m-%d")
        day = current_date.day
        month = current_date.month
        year = current_date.year
        quarter = (month - 1) // 3 + 1
        
        date_records.append((date_key, calendar_date, day, month, year, quarter))
        current_date += timedelta(days=1)
        
    try:
        cursor.execute("BEGIN TRANSACTION;")
        cursor.executemany(
            """
            INSERT OR REPLACE INTO dim_date (date_key, calendar_date, day, month, year, quarter)
            VALUES (?, ?, ?, ?, ?, ?);
            """,
            date_records
        )
        conn.commit()
        print(f"Successfully generated and inserted {len(date_records)} rows into dim_date.")
    except Exception as e:
        conn.rollback()
        print(f"Error generating Date Dimension: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    generate_dim_date()
