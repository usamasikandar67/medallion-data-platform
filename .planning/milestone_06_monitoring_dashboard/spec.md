# Milestone 6 Spec: Monitoring & Dashboard

## Objective
Design and implement platform observability (execution logging, data validation alerts, run duration tracking) and create structured dashboard views on the Gold analytical schema to serve executive reporting needs.

---

## Business Problem
Even with data quality tests, a data pipeline can degrade or fail due to operational issues (e.g. out-of-memory errors, API failures, high latency, missed SLA deadlines). Without historical run metadata, administrators cannot identify performance bottlenecks or track system health trends. Concurrently, business stakeholders require an intuitive, aggregated visualization of the Gold layer data (sales volume, active customers, order status distribution) to drive operational decisions.

---

## Functional Requirements
1. **Metadata Monitoring Logging**:
   - Establish an execution tracking table `pipeline_runs` logging:
     - `run_id` (UUID), `pipeline_name` (e.g., Bronze Ingest, Silver Merge, Gold Load).
     - `start_time`, `end_time`, `duration_seconds`.
     - `status` (`SUCCESS`, `FAILED`).
     - `records_processed`, `records_failed` (quarantined).
     - `error_message` (if FAILED).
2. **Alert Triggering (Data Quality Observability)**:
   - Compile data quality test results from dbt run logs.
   - If test errors exceed threshold values, write warning messages to local files or stdout logs acting as alert endpoints.
3. **Executive Dashboard Layer**:
   - Build optimized SQL Views or tables inside Gold to serve key business metrics:
     - **Revenue Metrics**: Total order value by date, week, and quarter.
     - **Customer Distributions**: Order counts by customer state and status.
     - **Historical Cohorts**: Track changes in order status patterns based on customer profile address revisions.
   - Create a lightweight simulation dashboard script that outputs formatted console metrics or a local HTML page containing visualizations of these KPI views.

---

## Non-functional Requirements
- **Low Overhead**: Logging execution stats must not increase pipeline runtime by more than 2%.
- **Query Performance**: Dashboard queries should run in < 1 second using indexed materialized structures.
- **Accuracy**: Aggregations in the dashboard views must exactly match the source transactions when sum-audited.

---

## Inputs
- **Gold Tables**: `dim_customers`, `fact_orders`, `dim_date`.
- **System Metrics**: OS level execution stats, dbt build targets.

---

## Outputs
- **Monitoring Table**: `pipeline_runs` record log in `data/warehouse.db`.
- **Analytical Views**:
  - `vw_executive_summary`
  - `vw_customer_sales_by_state`
- **Dashboard Application**: Visual rendering of KPIs (terminal or static HTML page).

---

## Architecture Decisions
- **Decoupled Monitoring**: Store `pipeline_runs` inside the same analytical warehouse database but under a distinct schema prefix (`monitor_`) to keep auditing datasets isolated from primary business tables.
- **Reporting layer**: Use SQL views (`CREATE VIEW`) for the dashboard aggregates instead of heavy tables. Since the Gold layer is updated in batches, views ensure that the dashboard always queries live, up-to-date analytical states.

---

## Dependencies
- Language runtimes: Python / HTML / CSS / JS.
- Visualizations: Light chart framework (e.g., Chart.js or plot libraries like matplotlib) if rendering HTML.

---

## Acceptance Criteria
- [ ] Running any ingestion or loading pipeline writes a tracking log to `pipeline_runs`.
- [ ] Failed pipeline runs correctly capture error messages and duration in `pipeline_runs`.
- [ ] Analytical dashboard views (`vw_executive_summary`, `vw_customer_sales_by_state`) return accurate aggregates matching raw order data.
- [ ] The dashboard script executes and successfully renders key metrics (sales totals, customer trends).

---

## Risks
- **Inflated Log Storage**: Continuous run logs can accumulate over time, eating up database disk space.
  - *Mitigation*: Configure a retention policy script (e.g., delete log records older than 90 days).

---

## Deliverables
1. DDL for monitoring schema (`scripts/create_monitoring_schema.sql`).
2. Logging wrapper function inside ingestion scripts.
3. SQL Views definition (`scripts/create_dashboard_views.sql`).
4. Dashboard UI/Console Script (`scripts/render_dashboard.py`).

---

## Future Improvements
- Integrate Prometheus and Grafana to build automated metric scrapers, charting runtime trends and firing webhook alerts to Slack or PagerDuty.
