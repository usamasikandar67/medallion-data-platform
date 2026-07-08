# Goal
Implement the DDL scripts to define Gold layer Star Schema tables and build the python/SQL loading scripts that generate dim_customers, dim_date, and fact_orders with historical surrogate key alignment.

# Implementation Steps
1. Create table definitions script `scripts/create_gold_tables.sql` containing:
   - `dim_customers` with fields: `customer_sk` (PK), `customer_id`, `first_name`, `last_name`, `email`, `state`, `valid_from`, `valid_to`, `is_current`.
   - `dim_date` with fields: `date_key` (PK), `calendar_date`, `day`, `month`, `year`, `quarter`.
   - `fact_orders` with fields: `order_id` (PK), `customer_sk` (FK), `date_key` (FK), `order_amount`, `order_status`, `created_at`.
2. Create script `scripts/generate_dim_date.py` to generate date dimensions from 2025-01-01 to 2030-12-31, inserting them into `dim_date`.
3. Create script `scripts/load_gold.py` to process conformed Silver data:
   - Read from `silver_customers`. Cleanse and load into `dim_customers` directly, including the default row for `customer_sk = -1` (First Name: "Unknown", Last Name: "Customer").
   - Read from `silver_orders` and join to `dim_customers` on:
     `silver_orders.customer_id = dim_customers.customer_id`
     AND `silver_orders.created_at >= dim_customers.valid_from`
     AND `silver_orders.created_at < dim_customers.valid_to`.
   - Resolve `date_key` by formatting the order's `created_at` timestamp as YYYYMMDD and joining to `dim_date.date_key`.
   - If no matching customer version is found, map `customer_sk = -1`.
   - Upsert the structured records into `fact_orders`.

# Validation Checklist
- [ ] Run DDL scripts and confirm tables `dim_customers`, `dim_date`, and `fact_orders` are created in `data/warehouse.db`.
- [ ] Run `python scripts/generate_dim_date.py` and confirm `dim_date` is populated (e.g. check row count > 2000).
- [ ] Run `python scripts/load_gold.py` to execute the dimensional loading.
- [ ] Execute validation query verifying that orders placed by a customer prior to an address change link to the old surrogate key version, and orders placed after link to the new surrogate key version.
- [ ] Execute validation query checking for orphaned rows (assert count of `fact_orders` rows where `customer_sk NOT IN (SELECT customer_sk FROM dim_customers)` is 0).

# Rollback Plan
- Drop Gold tables (`dim_customers`, `dim_date`, `fact_orders`) from the SQLite warehouse database.
- Remove date generation and loading python scripts using Git checkout.

# Expected Git Commit Message
feat(gold): implement fact and dimension analytical model
