# Milestone 2 Spec: Bronze Layer (Raw Storage & Ingestion)

## Objective
Design and implement the Bronze layer ingestion pipeline to collect raw change feed payloads from the CDC buffer directory and store them in an append-only, partitioned structure while capturing metadata.

---

## Business Problem
Raw CDC stream data needs to be stored in a durable, immutable format to guarantee data lineage and auditability. If transformation bugs occur downstream in the Silver or Gold layers, developers must be able to completely rebuild the database state starting from the raw history. Ingesting this data without structural alterations, while preserving partition layouts and ingestion time metadata, ensures we retain a perfect historical archive of all source activities.

---

## Functional Requirements
1. **Append-Only Ingestion**:
   - Ingest files from `data/cdc_buffer/` on a periodic or micro-batch basis.
   - Do not perform any filtering, cleansing, column renaming, or data type conversion.
   - Append raw records directly to the Bronze destination tables.
2. **System Metadata Enrichment**:
   - For every ingested record, append two tracking columns:
     - `_bronze_ingested_at`: The UTC timestamp when the ingestion job processed the record.
     - `_bronze_batch_id`: A unique batch or job execution run identifier (UUID or run ID).
3. **Partitioning Strategy**:
   - Partition Bronze files on disk by ingestion date (e.g., `ingest_date=YYYY-MM-DD`).
4. **Archival & Deduplication Control**:
   - Archive successfully ingested raw source files into a `data/cdc_archive/` folder to prevent duplicate ingestion in subsequent runs.

---

## Non-functional Requirements
- **Idempotence**: Re-running the ingestion for a specific batch ID must not result in duplicate records if the transaction fails mid-process.
- **Throughput**: Ingestion pipeline should handle peak volume events (e.g., 10,000 mutations per minute) within SLA limits (< 2 minutes ingestion latency).
- **Storage Format**: Stored as Parquet or Delta Lake tables locally, allowing columnar query efficiency and schema enforcement.

---

## Inputs
- Raw JSON change files inside `data/cdc_buffer/` containing:
  - Source entity payload (customers, orders).
  - CDC metadata (`_change_type`, `_commit_timestamp`, `_row_sequence_id`).

---

## Outputs
- **Bronze Raw Tables**:
  - `bronze_customers`: Parquet or Delta table structured with raw columns, partitions, and system metadata.
  - `bronze_orders`: Parquet or Delta table structured with raw columns, partitions, and system metadata.

---

## Architecture Decisions
- **Storage Format**: Parquet is chosen as the local storage format to optimize downstream read operations and mimic production cloud data lake architectures (like AWS S3 or ADLS Gen2).
- **Archiving Pattern**: File moves from `cdc_buffer/` to `cdc_archive/` are executed atomically after a batch commit completes to guarantee "exactly-once" load guarantees in local development.

---

## Dependencies
- Language runtimes: Python (with pandas or PySpark).
- Storage framework: `pyarrow` and `fastparquet` for Parquet files.

---

## Acceptance Criteria
- [ ] Ingestion reads all JSON files from `data/cdc_buffer/` and converts them into the partitioned Bronze storage.
- [ ] Stored Bronze tables include `_bronze_ingested_at` and `_bronze_batch_id` metadata fields.
- [ ] Files are stored in the directory structure: `data/bronze/{entity}/ingest_date=YYYY-MM-DD/*.parquet`.
- [ ] Ingested files are moved from `data/cdc_buffer/` to `data/cdc_archive/`.
- [ ] Re-running the ingestion with an empty buffer does not generate empty partitions or error out.

---

## Risks
- **Schema Drift**: If a new column is added in the source database, the ingestion might fail if the Parquet writer does not support schema evolution.
  - *Mitigation*: Enable schema merging or write schema-agnostic variant/JSON blocks in Bronze, conformed downstream.

---

## Deliverables
1. Ingestion Pipeline Script (`scripts/ingest_bronze.py`).
2. Local Storage Directories (`data/bronze/customers/` and `data/bronze/orders/`).
3. Partition utilities configuration.

---

## Future Improvements
- Implement Delta Lake instead of standard Parquet to leverage ACID transactions and automatic schema enforcement/evolution at the storage layer.
