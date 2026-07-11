-- ==============================================================================
-- Databricks SQL Dashboard Queries
-- 
-- Usage: Create a new Databricks SQL Dashboard and use these parameterized 
-- queries to build visualizations (Line Charts, Bar Charts, Counters).
-- ==============================================================================

-- 1. Daily Revenue Trend (Line Chart)
-- Shows total revenue over time, grouped by date.
SELECT 
    order_date,
    total_revenue
FROM workspace.gold.agg_daily_sales
ORDER BY order_date ASC;

-- 2. Daily Orders Volume (Bar Chart)
-- Shows the count of orders processed per day.
SELECT 
    order_date,
    total_orders
FROM workspace.gold.agg_daily_sales
ORDER BY order_date ASC;

-- 3. Revenue by State (Choropleth Map / Bar Chart)
-- Visualizes geographical revenue distribution.
SELECT 
    state,
    state_revenue,
    state_orders
FROM workspace.gold.agg_revenue_trends
ORDER BY state_revenue DESC;

-- 4. Customer Growth (Area Chart)
-- Displays the acquisition of new customers over time.
SELECT 
    join_date,
    new_customers,
    SUM(new_customers) OVER (ORDER BY join_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_customers
FROM workspace.gold.agg_customer_growth
ORDER BY join_date ASC;

-- 5. Top 10 Customers by Lifetime Value (Table)
-- Highlights the most valuable customers.
SELECT 
    c.first_name,
    c.last_name,
    c.state,
    COUNT(f.order_id) as total_orders,
    SUM(f.order_amount) as lifetime_value
FROM workspace.gold.fact_orders f
JOIN workspace.gold.dim_customers c ON f.customer_sk = c.customer_id
GROUP BY c.first_name, c.last_name, c.state
ORDER BY lifetime_value DESC
LIMIT 10;

-- 6. Pipeline Data Quality Status (Donut Chart)
-- Shows the proportion of successfully processed vs quarantined records.
SELECT 
    CASE 
        WHEN records_quarantined > 0 THEN 'Quarantined'
        ELSE 'Clean'
    END as record_status,
    SUM(records_processed) as total_clean,
    SUM(records_quarantined) as total_quarantined
FROM workspace.medallion.pipeline_audit_log
WHERE start_time >= current_date() - INTERVAL 7 DAYS
GROUP BY 1;

-- 7. Pipeline Execution Duration Trend (Line Chart)
-- Monitors Databricks Workflow execution times for performance regressions.
SELECT 
    start_time,
    pipeline_name,
    layer,
    duration_seconds
FROM workspace.medallion.pipeline_audit_log
WHERE status = 'SUCCESS'
ORDER BY start_time DESC;
