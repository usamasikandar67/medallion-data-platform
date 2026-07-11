# Databricks Medallion Architecture Specification

## Overview
This document specifies the technical architecture for the Databricks PySpark Medallion data platform, migrating the local SQLite approach into an enterprise-grade Unity Catalog implementation.

## Schema Definitions

### Bronze Layer (`workspace.bronze`)
- Contains raw CDC representations of OLTP databases.
- Contains historical JSON payloads processed into Delta tables.

### Silver Layer (`workspace.silver`)
- **`silver_customers`**: SCD Type 2 dimension.
  - Columns: `customer_id`, `first_name`, `last_name`, `email`, `state`, `_commit_timestamp`, `is_current`, `valid_from`, `valid_to`, `is_deleted`, `_silver_processed_at`
- **`silver_orders`**: Cleaned upserted orders.
- **`quarantine_*`**: Failed records with `_quarantine_reason` and `_quarantined_at`.

### Gold Layer (`workspace.gold`)
- **`dim_customers`**: Filtered active customers `WHERE is_current = 1 AND is_deleted = 0`.
- **`fact_orders`**: Orders enriched with `customer_sk`.
- **`agg_daily_sales`**: Daily revenue aggregations.
- **`agg_revenue_trends`**: Revenue grouped by geographic state.

## Processing Logic
- **CDC Merges**: Handled via `DeltaTable.merge()` APIs.
- **SCD2 Implementation**: A multi-step join identifies changes in source, stages inserts and updates into a unified DataFrame, and performs a single MERGE operation that retires old records and inserts new active rows.
- **Optimization**: `OPTIMIZE` and `ZORDER BY` applied natively in `databricks_gold_layer.py`.
