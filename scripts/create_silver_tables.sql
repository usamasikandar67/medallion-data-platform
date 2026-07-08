-- Create Silver Layer Conformed Tables
-- Target Database: data/warehouse.db

-- SCD Type 2 Customers Table
CREATE TABLE IF NOT EXISTS silver_customers (
    customer_sk TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    state TEXT,
    valid_from TEXT NOT NULL,
    valid_to TEXT NOT NULL,
    is_current INTEGER NOT NULL, -- 1 for active, 0 for inactive
    is_deleted INTEGER NOT NULL, -- 1 for soft-deleted, 0 for active
    _bronze_batch_id TEXT NOT NULL
);

-- Indices for performance
CREATE INDEX IF NOT EXISTS idx_silver_customers_id ON silver_customers(customer_id);
CREATE INDEX IF NOT EXISTS idx_silver_customers_current ON silver_customers(is_current);

-- Conformed Orders Table
CREATE TABLE IF NOT EXISTS silver_orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT,
    order_amount REAL,
    order_status TEXT,
    created_at TEXT,
    updated_at TEXT,
    _bronze_batch_id TEXT NOT NULL
);
