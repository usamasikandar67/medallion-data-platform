-- Create Dashboard Analytics SQL Views
-- Target Database: data/warehouse.db

-- 1. Executive Summary: Sales Totals, Order Count, and Active Customer Counts
DROP VIEW IF EXISTS vw_executive_summary;
CREATE VIEW vw_executive_summary AS
SELECT 
    ROUND(SUM(order_amount), 2) as total_sales,
    COUNT(order_id) as total_orders,
    COUNT(DISTINCT customer_sk) as unique_customers
FROM fact_orders;

-- 2. Sales by State: Grouping revenue split by state codes
DROP VIEW IF EXISTS vw_sales_by_state;
CREATE VIEW vw_sales_by_state AS
SELECT 
    COALESCE(c.state, 'UNKNOWN') as state_code,
    ROUND(SUM(f.order_amount), 2) as sales_amount,
    COUNT(f.order_id) as order_count
FROM fact_orders f
LEFT JOIN dim_customers c ON f.customer_sk = c.customer_sk
GROUP BY state_code
ORDER BY sales_amount DESC;

-- 3. Order Status Breakdown: Counts of orders by shipping status
DROP VIEW IF EXISTS vw_order_status_breakdown;
CREATE VIEW vw_order_status_breakdown AS
SELECT 
    order_status,
    COUNT(order_id) as order_count
FROM fact_orders
GROUP BY order_status
ORDER BY order_count DESC;

-- 4. Unresolved Orders (Observability check)
DROP VIEW IF EXISTS vw_unresolved_orders;
CREATE VIEW vw_unresolved_orders AS
SELECT 
    COUNT(order_id) as unresolved_orders_count,
    ROUND((COUNT(order_id) * 100.0) / (SELECT COUNT(*) FROM fact_orders), 2) as unresolved_percentage
FROM fact_orders
WHERE customer_sk = '-1';
