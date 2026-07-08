# Milestone 3 Spec: Silver Layer (Cleanse, Standardize, and MERGE-based SCD Type 2)

## Objective
Design and implement the Silver layer conformed tables. Establish staging transformation logic (data typing, NULL handling, deduplication) and apply a SQL-based `MERGE` statement to manage Slowly Changing Dimensions (SCD) Type 2 tracking for the customers entity, and transactional history for orders.

---

## Business Problem
Raw CDC logs are unstructured, contain duplicates, and reflect mutations over time. Analysts and business applications cannot query raw change logs directly because they need to see either:
1. The *current state* of a record (e.g., a customer's active address).
2. The *historical state* of a record at a specific point in time (e.g., where did customer X live when order Y was placed?).
Implementing MERGE-based SCD Type 2 in the Silver layer structures this transactional history into a conformed, version-controlled table with `valid_from`, `valid_to`, and `is_current` columns.

---

## Functional Requirements
1. **Clean & Standardize (Staging)**:
   - Read incremental datasets from the Bronze tables.
   - Enforce data types (e.g., cast string timestamps to SQL timestamps, numeric values to decimals).
   - Normalize string fields (trim whitespace, lower/uppercase formatting).
   - Filter out records missing critical business keys (quarantine handling).
2. **SCD Type 2 Lifecycle Logic**:
   - Track mutations on the `customers` table using the natural business key `customer_id`.
   - Maintain historical records utilizing:
     - `valid_from`: Timestamp when the record version became active.
     - `valid_to`: Timestamp when the record version was superseded (or NULL/`9999-12-31` if active).
     - `is_current`: Boolean indicating the current active version.
   - **Merge Mechanics**:
     - Identify incoming records that are updates to existing keys.
     - Close existing active rows by setting `valid_to = _commit_timestamp` and `is_current = False`.
     - Insert the new row version with `valid_from = _commit_timestamp`, `valid_to = 9999-12-31`, and `is_current = True`.
     - Insert brand new records with `valid_from = _commit_timestamp`, `valid_to = 9999-12-31`, and `is_current = True`.
     - For deletes: Set the active row's `valid_to = _commit_timestamp` and `is_current = False`.
3. **Transaction History Logic**:
   - For `orders`, append new events or update current order status versions, ensuring only the latest state is active if duplicates exist in the same micro-batch.

---

## Non-functional Requirements
- **ACID Compliance**: The merge logic must execute inside a transaction. A failure mid-merge must rollback the entire batch.
- **Idempotence**: Running the Silver pipeline multiple times with the same input Bronze batch must result in the same database state (no duplicate historical versions).
- **Latency**: The Silver execution must complete within < 3 minutes for a standard batch size of 50,000 records.

---

## Inputs
- **Bronze Tables**:
  - `bronze_customers` (Parquet files with metadata).
  - `bronze_orders` (Parquet files with metadata).

---

## Outputs
- **Silver Conformed Tables**:
  - `silver_customers`: SCD Type 2 conformed table.
  - `silver_orders`: Standardized orders history table.

---

## Architecture Decisions
- **Merge Method**: A standard two-step MERGE design will be implemented. Because most standard SQL databases do not support updating and inserting in a single simple `MERGE` statement for SCD Type 2, the pipeline will:
  1. Identify keys to be updated and perform an `UPDATE` statement to close existing rows.
  2. Perform an `INSERT` statement to append the new row versions and new records.
- **Surrogate Keys**: Generate a deterministic hash surrogate key (`customer_sk`) in the Silver table using a hash (e.g., MD5) of `customer_id` and `valid_from` to uniquely identify each row version.

---

## Dependencies
- Language runtimes: Python / SQL.
- Storage engine: SQL Database (e.g., SQLite, PostgreSQL) or Spark SQL/Delta.

---

## Acceptance Criteria
- [ ] Silver customer table maintains multiple rows for a single `customer_id` if their attributes changed over time.
- [ ] A superseded row has `is_current = False` and `valid_to` set to the subsequent row's `valid_from` (exact timestamp match).
- [ ] The current active row has `is_current = True` and `valid_to = '9999-12-31 00:00:00'`.
- [ ] Re-running the pipeline with the same Bronze inputs does not create duplicate versions or update closed rows.
- [ ] Invalid or incomplete rows (missing `customer_id`) are quarantined rather than crashing the execution.

---

## Risks
- **Late-Arriving Data**: CDC payloads arriving out of temporal sequence could write a `valid_from` older than an existing active record.
  - *Mitigation*: Ensure the merge logic compares `_commit_timestamp` and only updates if the incoming timestamp is strictly greater than the target's current `valid_from`.

---

## Deliverables
1. Staging and Merge SQL scripts or python processing scripts (`scripts/load_silver.py`).
2. Table creation scripts for `silver_customers` and `silver_orders` (using DDL statements).

---

## Future Improvements
- Automate partition pruning on Silver tables to optimize performance as history scales to millions of rows.
