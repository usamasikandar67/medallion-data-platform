#!/usr/bin/env python3
import os
import json
import sqlite3
from datetime import datetime, timezone

WAREHOUSE_DB = "data/warehouse.db"
OUTPUT_JSON = "data/dashboard_stats.json"
OUTPUT_JS = "data/dashboard_data.js"

def generate_data():
    if not os.path.exists(WAREHOUSE_DB):
        print(f"Error: Warehouse database not found at {WAREHOUSE_DB}")
        return

    conn = sqlite3.connect(WAREHOUSE_DB)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    stats = {}

    try:
        # 1. Executive Summary
        cursor.execute("SELECT * FROM vw_executive_summary;")
        row = cursor.fetchone()
        stats["executive_summary"] = {
            "total_sales": row["total_sales"] or 0.0,
            "total_orders": row["total_orders"] or 0,
            "unique_customers": row["unique_customers"] or 0
        }

        # 2. Sales by State
        cursor.execute("SELECT * FROM vw_sales_by_state;")
        stats["sales_by_state"] = [dict(r) for r in cursor.fetchall()]

        # 3. Order Status Breakdown
        cursor.execute("SELECT * FROM vw_order_status_breakdown;")
        stats["order_status_breakdown"] = [dict(r) for r in cursor.fetchall()]

        # 4. Unresolved Orders Observability
        cursor.execute("SELECT * FROM vw_unresolved_orders;")
        u_row = cursor.fetchone()
        stats["unresolved_orders"] = {
            "count": u_row["unresolved_orders_count"] or 0,
            "percentage": u_row["unresolved_percentage"] or 0.0
        }
        
        # 5. Pipeline execution runs logging
        cursor.execute("SELECT * FROM monitor_pipeline_runs ORDER BY start_time DESC LIMIT 10;")
        stats["pipeline_runs"] = [dict(r) for r in cursor.fetchall()]
        
        # Add generated timestamp
        stats["generated_at"] = datetime.now(timezone.utc).isoformat()

        # Write to JSON file
        os.makedirs(os.path.dirname(OUTPUT_JSON), exist_ok=True)
        with open(OUTPUT_JSON, 'w') as f:
            json.dump(stats, f, indent=4)
        print(f"Successfully generated JSON dashboard stats at {OUTPUT_JSON}")

        # Write to JS file (CORS bypass for local loading)
        with open(OUTPUT_JS, 'w') as f:
            f.write(f"const dashboardData = {json.dumps(stats, indent=4)};")
        print(f"Successfully generated JS data wrapper at {OUTPUT_JS}")

        # Output to console
        print("\n=== EXECUTIVE DATA METRICS SUMMARY ===")
        print(f"Total Revenue:   ${stats['executive_summary']['total_sales']:,}")
        print(f"Total Orders:    {stats['executive_summary']['total_orders']:,}")
        print(f"Active Customers: {stats['executive_summary']['unique_customers']:,}")
        print(f"Unresolved Orders (Orphaned): {stats['unresolved_orders']['count']} ({stats['unresolved_orders']['percentage']}%)")
        print("======================================\n")

    except Exception as e:
        print(f"Error generating dashboard dataset: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    generate_data()
