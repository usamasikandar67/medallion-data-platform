# Architectural Decision Record: Databricks PySpark Migration

## Context
The platform originated as a local Python script interacting with SQLite, relying on Pandas and hardcoded SQL scripts for dbt execution. While sufficient for MVP, the lack of distributed processing, decoupled storage (Delta Lake), and robust orchestration necessitated a migration to Databricks.

## Decisions Made
1. **PySpark over Pandas**: Replaced all Pandas DataFrame manipulations with PySpark DataFrames to enable distributed execution and leverage Spark Catalyst Optimizer.
2. **Delta Lake `MERGE` over SQL UPSERT**: Replaced bespoke SQLite `INSERT OR REPLACE` and complex dbt snapshot macros with native Databricks Delta `MERGE INTO` operations, drastically simplifying SCD Type 2 management.
3. **Programmatic Catalog Discovery**: Eliminated hardcoded tables in the Bronze layer. `databricks_silver_layer.py` now queries Unity Catalog to process dynamically discovered tables, making the pipeline adaptable to new data streams.
4. **Data Quality Quarantines in Spark**: Integrated PySpark `.when().otherwise()` chaining for DQ assertions, avoiding the overhead of external testing frameworks like Great Expectations while retaining strict data governance.
5. **Databricks Workflows over Airflow (Local)**: Replaced the local Docker-based Airflow DAGs with a `databricks_workflow.json` configuration, migrating orchestration directly onto the Databricks control plane.
