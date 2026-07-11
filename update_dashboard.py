import json
import subprocess
import uuid

# Define the queries
queries = [
  {"name": "Medallion Daily Orders Volume", "query": "SELECT order_date, total_orders FROM workspace.gold.agg_daily_sales ORDER BY order_date ASC;"},
  {"name": "Medallion Revenue by State", "query": "SELECT state, state_revenue, state_orders FROM workspace.gold.agg_revenue_trends ORDER BY state_revenue DESC;"},
  {"name": "Medallion Customer Growth", "query": "SELECT join_date, new_customers, SUM(new_customers) OVER (ORDER BY join_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_customers FROM workspace.gold.agg_customer_growth ORDER BY join_date ASC;"},
  {"name": "Medallion Top 10 Customers LTV", "query": "SELECT c.first_name, c.last_name, c.state, COUNT(f.order_id) as total_orders, SUM(f.order_amount) as lifetime_value FROM workspace.gold.fact_orders f JOIN workspace.gold.dim_customers c ON f.customer_sk = c.customer_id GROUP BY c.first_name, c.last_name, c.state ORDER BY lifetime_value DESC LIMIT 10;"},
  {"name": "Medallion Data Quality Status", "query": "SELECT CASE WHEN records_quarantined > 0 THEN 'Quarantined' ELSE 'Clean' END as record_status, SUM(records_processed) as total_clean, SUM(records_quarantined) as total_quarantined FROM workspace.medallion.pipeline_audit_log WHERE start_time >= current_date() - INTERVAL 7 DAYS GROUP BY 1;"},
  {"name": "Medallion Execution Trend", "query": "SELECT start_time, pipeline_name, layer, duration_seconds FROM workspace.medallion.pipeline_audit_log WHERE status = 'SUCCESS' ORDER BY start_time DESC;"}
]

# Build the datasets payload for Lakeview
datasets = []
for q in queries:
    datasets.append({
        "name": str(uuid.uuid4()),
        "displayName": q["name"],
        "query": q["query"]
    })

serialized_dashboard = {
    "pages": [
        {
            "name": str(uuid.uuid4()),
            "displayName": "Main Page",
            "widgets": []
        }
    ],
    "datasets": datasets
}

dashboard_id = "01f17c5579d1163b85a5b6f585a64b34"

payload = {
    "display_name": "Medallion Data Platform",
    "serialized_dashboard": json.dumps(serialized_dashboard)
}

with open("payload.json", "w") as f:
    json.dump(payload, f)

print("Updating Databricks Lakeview Dashboard...")
res = subprocess.run(["databricks", "api", "patch", f"/api/2.0/lakeview/dashboards/{dashboard_id}", "--json", "@payload.json"], capture_output=True, text=True)
if res.returncode == 0:
    print("Success!")
else:
    print("Failed to update dashboard.")
    print(res.stderr)
