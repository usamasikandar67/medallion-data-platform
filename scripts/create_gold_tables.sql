-- Create Gold Layer Analytical Star Schema Tables
-- Target Database: data/warehouse.db

-- Customers Dimension (SCD Type 2 Preserved)
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_sk TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    state TEXT,
    valid_from TEXT NOT NULL,
    valid_to TEXT NOT NULL,
    is_current INTEGER NOT NULL
);

-- Date Dimension
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY, -- YYYYMMDD
    calendar_date TEXT NOT NULL,  -- YYYY-MM-DD
    day INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL
);

-- Orders Fact Table
CREATE TABLE IF NOT EXISTS fact_orders (
    order_id TEXT PRIMARY KEY,
    customer_sk TEXT NOT NULL,
    date_key INTEGER NOT NULL,
    order_amount REAL NOT NULL,
    order_status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(customer_sk) REFERENCES dim_customers(customer_sk),
    FOREIGN KEY(date_key) REFERENCES dim_date(date_key)
);

-- Indexing for Fact joins
CREATE INDEX IF NOT EXISTS idx_fact_orders_cust_sk ON fact_orders(customer_sk);
CREATE INDEX IF NOT EXISTS idx_fact_orders_date_key ON fact_orders(date_key);
