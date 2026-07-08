# Goal
Implement SQL views for business intelligence aggregates, configure database tracking schemas for run executions, update ingestion wrappers to write pipeline run logs, and write a dashboard generator.

# Implementation Steps
1. Create SQL script `scripts/create_monitoring_schema.sql` defining table `monitor_pipeline_runs`:
   - fields: `run_id`, `pipeline_name`, `start_time`, `end_time`, `duration_seconds`, `status`, `records_processed`, `error_message`.
2. Create SQL script `scripts/create_dashboard_views.sql` defining views:
   - `vw_executive_summary` (Total sales amount, total order count, unique active customer counts).
   - `vw_sales_by_state` (Aggregate sales split by customer geography state codes).
   - `vw_order_status_breakdown` (Counts of orders grouped by current shipping states).
3. Update pipeline scripts (`ingest_bronze.py`, `load_silver.py`, `load_gold.py`) or create a decorator script `scripts/pipeline_decorator.py` that:
   - Starts a database connection.
   - Inserts a record into `monitor_pipeline_runs` marking status as 'RUNNING'.
   - Runs the main transformation logic.
   - Logs final status ('SUCCESS' or 'FAILED'), duration, and records written upon completion.
4. Create script `scripts/generate_dashboard_data.py` that runs queries on the views, exports findings to a JSON file `data/dashboard_stats.json`, and outputs KPI summaries to standard output.
5. Create a responsive web document `dashboard/index.html` displaying these KPI stats visually with charts.

# Validation Checklist
- [ ] Run scripts generating logging tables and SQL Views. Verify views execute and return rows.
- [ ] Run `python scripts/ingest_bronze.py` and confirm a new record appears in `monitor_pipeline_runs` detailing duration and SUCCESS status.
- [ ] Cause a database read error manually and confirm the corresponding pipeline run records status = 'FAILED' and writes the stack trace to `error_message`.
- [ ] Execute `python scripts/generate_dashboard_data.py` and verify `data/dashboard_stats.json` is generated.
- [ ] Open `dashboard/index.html` in a web browser and confirm chart shapes, customer counts, and revenue indicators match the SQL aggregates.

# Rollback Plan
- Drop views (`vw_executive_summary`, `vw_sales_by_state`, `vw_order_status_breakdown`).
- Drop tracking table `monitor_pipeline_runs`.
- Delete the dashboard folder and data export files.
- Revert modifications to processing wrapper scripts.

# Expected Git Commit Message
feat(monitor): implement execution logging and dashboard queries
