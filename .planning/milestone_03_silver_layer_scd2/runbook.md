# Goal
Implement the DDL scripts to define Silver layer conformed tables and create a loading process that cleanses incoming Bronze data and applies SCD Type 2 logic using SQLite transactional commands.

# Implementation Steps
1. Create table definitions script `scripts/create_silver_tables.sql` containing:
   - `silver_customers` with fields: `customer_sk` (PK), `customer_id`, `first_name`, `last_name`, `email`, `state`, `valid_from`, `valid_to`, `is_current`, `is_deleted`, `_bronze_batch_id`.
   - `silver_orders` with fields: `order_id` (PK), `customer_id`, `order_amount`, `order_status`, `created_at`, `updated_at`, `_bronze_batch_id`.
2. Create script `scripts/load_silver.py` that processes incremental data:
   - Read un-processed records from Bronze Parquet folders.
   - Clean data: trim whitespaces, cast string timestamps to SQL DATETIME, check for NULL customer keys (redirecting them to a `data/quarantine/` folder).
   - Deduplicate within the incoming batch: if the same customer has multiple updates, sort them by `_commit_timestamp` and `_row_sequence_id` ascending to process changes in order.
   - For every customer record in the batch:
     - Check if the `customer_id` already exists in `silver_customers`.
     - If it exists, and the incoming change is an `UPDATE` or `DELETE`, close the current active row (`valid_to = incoming._commit_timestamp`, `is_current = False`).
     - If the incoming change is not a `DELETE`, insert a new row version with a deterministic hash surrogate key, `valid_from = incoming._commit_timestamp`, `valid_to = '9999-12-31 23:59:59'`, and `is_current = True`.
   - For every order record:
     - Perform a standard upsert (`INSERT OR REPLACE`) based on `order_id` to ensure only the latest state is captured.
3. Wrap the merge process in a SQL database transaction (`BEGIN TRANSACTION` and `COMMIT`).

# Validation Checklist
- [ ] Run the creation script and confirm tables `silver_customers` and `silver_orders` are created in `data/warehouse.db`.
- [ ] Run `python scripts/load_silver.py` to ingest the first Bronze batch. Check that all rows have `is_current = True` and `valid_to = '9999-12-31 23:59:59'`.
- [ ] Run the database simulator to update email addresses or states for existing customer records, run the Bronze ingestion, and then execute the Silver loader.
- [ ] Verify that updated customers now have two records: the historical record (`is_current = False`, `valid_to = change_timestamp`) and the new active record (`is_current = True`, `valid_to = '9999-12-31 23:59:59'`).
- [ ] Validate that customer surrogate keys (`customer_sk`) are unique and successfully hashed.
- [ ] Check if records missing keys are written to the quarantine path.

# Rollback Plan
- Drop the SQLite tables `silver_customers` and `silver_orders` inside `data/warehouse.db` or restore from a backup of `data/warehouse.db`.
- Clear the `data/quarantine/` directory.
- Revert python scripts and SQL configurations using Git checkout.

# Expected Git Commit Message
feat(silver): implement staging and merge-based scd type 2 updates
