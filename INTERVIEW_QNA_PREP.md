# Medallion Platform: Presentation Q&A Interview Prep Guide

This document contains targeted technical questions and answers to prepare you for your Friday night presentation and project defense.

---

## Category 1: Architecture & Medallion Design

### Q1: What is the core business problem this architecture solves?
* **Answer**: It prevents **historical data loss during source system mutations**. In typical databases, when a customer moves or an order status changes, old values are overwritten. This project captures every intermediate state change using Change Data Capture (CDC) and historical modeling (SCD Type 2), ensuring that financial reports, tax calculations, and auditable metrics remain perfectly accurate over time.
* **Code Reference**: Database triggers in [scripts/db_simulator.py](file:///Users/d-23-10623/Desktop/cdc/scripts/db_simulator.py) log changes to the source audit table `cdc_event_log`.

---

### Q2: Walk me through the Medallion layers. What happens in Bronze, Silver, and Gold?
* **Answer**:
  - **Bronze (Raw)**: Raw JSON CDC change logs are ingested, partitioned by date, and saved as immutable **Parquet files** on disk. It preserves the raw schema exactly as it arrived.
  - **Silver (Conformed)**: Cleanses data types, filters out garbage rows, deduplicates, and implements **Slowly Changing Dimensions (SCD Type 2)** to establish historical timelines.
  - **Gold (Analytical)**: Connects facts and dimensions in a **Kimball Star Schema**. It performs point-in-time temporal joins so that order transactions associate with the customer's state at the exact moment the sale occurred.
* **Code Reference**: Models under [transformations/models/marts/](file:///Users/d-23-10623/Desktop/cdc/transformations/models/marts/).

---

### Q3: Why did you use Surrogate Keys (SK) in the Gold layer instead of natural database IDs?
* **Answer**: Natural keys (like `customer_id` UUID) are not unique when tracking history. If Bob has three historical versions (representing changes to his address), he will have three rows in `dim_customers` with the same `customer_id`. By creating a **Surrogate Key** (e.g., `customer_sk` composed of `customer_id` + a hash or row sequence), we give each version a unique identifier. This allows the Fact table (`fact_orders`) to join precisely to the correct profile version.
* **Code Reference**: Surrogate key generation in [silver_customers.sql](file:///Users/d-23-10623/Desktop/cdc/transformations/models/marts/silver_customers.sql#L19-L21).

---

## Category 2: Data Processing & Coding Decisions

### Q4: Why did you choose Parquet format for Bronze storage instead of CSV or JSON?
* **Answer**: Parquet is a **column-oriented, compressed binary storage format**. It provides three major advantages:
  1. **Storage Efficiency**: Highly compressed, reducing disk usage.
  2. **Query Performance**: Database engines can read only the columns required for a query rather than scanning the entire file.
  3. **Schema Preservation**: Unlike CSVs, Parquet stores metadata schema definitions (data types) directly in the file headers, preventing type coercion bugs downstream.
* **Code Reference**: Parquet partition writing in [scripts/ingest_bronze.py](file:///Users/d-23-10623/Desktop/cdc/scripts/ingest_bronze.py#L90-L105).

---

### Q5: Explain how Slowly Changing Dimensions (SCD Type 2) is calculated in your code.
* **Answer**: We use the SQL window function `LEAD()` ordered by the mutation timestamp (`_commit_timestamp`). 
  - For each customer profile, the current version's start time (`valid_from`) is set to its change timestamp. 
  - Its end time (`valid_to`) is set to the start time of the next modification. 
  - If no subsequent modification exists, `valid_to` defaults to `9999-12-31` and `is_current` is marked as `1`.
* **Code Reference**: Window logic in [silver_customers.sql](file:///Users/d-23-10623/Desktop/cdc/transformations/models/marts/silver_customers.sql#L33-L45).

---

## Category 3: dbt & Data Quality

### Q6: How does dbt help secure data quality? What tests did you write?
* **Answer**: dbt acts as a data quality gatekeeper. We write schemas tests in `schema.yml` to assert that:
  - Surrogate keys are **unique** and **never null**.
  - Status columns only contain **accepted values** (e.g., Pending, Shipped, Delivered, Cancelled).
  - Referential integrity is intact (**relationships** check between `fact_orders` and `dim_customers`).
  We also wrote a custom SQL assertion test verifying that order amounts are never negative. If a test fails during the build, the pipeline halts immediately.
* **Code Reference**: Schema assertions in [transformations/models/schema.yml](file:///Users/d-23-10623/Desktop/cdc/transformations/models/schema.yml).

---

### Q7: What is the difference between dbt models in the staging layer and the marts layer?
* **Answer**:
  - **Staging (`models/staging/`)**: Simply reads raw tables, casts data types, renames columns to conform to team standards, and does simple filters. It does not perform joins or aggregations.
  - **Marts (`models/marts/`)**: The business logic layer. This is where we run history computations (SCD Type 2), model Star Schema dimensions and facts, and run temporal point-in-time joins.

---

## Category 4: Orchestration & Docker

### Q8: What does Docker Compose solve in your architecture?
* **Answer**: It guarantees **environment parity**. Instead of configuring Airflow, Postgres, and Python packages manually on a local machine, Docker Compose boots up isolated containers matching our exact production versions with a single command (`docker compose up -d`). It ensures that code running locally behaves identically in cloud staging or production environments.
* **Code Reference**: Configured services in [docker-compose.yml](file:///Users/d-23-10623/Desktop/cdc/docker-compose.yml).

---

### Q9: Walk me through your Airflow DAG. What happens if a task in the middle fails?
* **Answer**: Our Airflow DAG (`medallion_pipeline_dag`) enforces a strict dependency chain:
  `CDC Extraction -> Bronze Ingest -> Bronze to SQLite -> dbt Run -> dbt Test -> Dashboard data generation`.
  If a task in the middle fails (e.g., `dbt Run`), downstream tasks (like tests or dashboard updates) are skipped to prevent corrupting reports. The failure is caught by our pipeline logger, which writes the error stack trace to the `monitor_pipeline_runs` table, displaying a warning badge on our dashboard.
* **Code Reference**: DAG tasks and dependencies in [orchestration/pipeline_dag.py](file:///Users/d-23-10623/Desktop/cdc/orchestration/pipeline_dag.py).

---

## Category 5: Observability & BI

### Q10: How does your observability dashboard handle "Late-Arriving Dimensions"?
* **Answer**: If an order arrives for a customer whose details have not yet synced to the warehouse, a standard join would discard the order (data loss). In `fact_orders`, we use a `COALESCE` statement that falls back to `customer_sk = -1` (Unknown Customer) if no match is found. Our SQL database exposes an analytical view `vw_unresolved_orders`. If unresolved rows are detected, the HTML dashboard raises a high-priority warning banner alerting administrators.
* **Code Reference**: Fallback key logic in [fact_orders.sql](file:///Users/d-23-10623/Desktop/cdc/transformations/models/marts/fact_orders.sql#L17) and the warning view in [scripts/create_dashboard_views.sql](file:///Users/d-23-10623/Desktop/cdc/scripts/create_dashboard_views.sql#L25-L35).

---

## Category 6: Databricks Integration

### Q11: How would you scale this local SQLite prototype to Databricks?
* **Answer**:
  1. **Storage (Bronze)**: Replace the local file paths with cloud object storage (AWS S3/ADLS). We would use **Databricks Autoloader** (`cloudFiles`) to ingest CDC JSON logs as they land in S3 and write them as raw Delta tables.
  2. **dbt Adapter**: Change the dbt adapter in `profiles.yml` from `dbt-sqlite` to `dbt-databricks` and connect it to a Serverless SQL Warehouse using Workspace tokens.
  3. **History (Silver)**: Enable **Delta Change Data Feed (CDF)** on Silver tables to automatically track transaction audits, and use Delta's native `MERGE` SQL queries for SCD Type 2 logic.
  4. **Orchestration**: Switch local Airflow tasks to trigger **Databricks Workflows** or Delta Live Tables (DLT) jobs.
