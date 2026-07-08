# Goal
Implement an ingestion pipeline that reads JSON files from the CDC buffer, enriches them with ingestion metadata, writes them as partitioned Parquet files under the Bronze directory, and archives the processed source files.

# Implementation Steps
1. Create a script named `scripts/ingest_bronze.py`.
2. Inside the script, list all files in `data/cdc_buffer/` matching `cdc_batch_*.json`.
3. If no files are found, log a message and exit gracefully.
4. Read all files into a unified dataframe (e.g., using Pandas or PySpark).
5. Add metadata fields:
   - `_bronze_ingested_at`: UTC timestamp string of current execution.
   - `_bronze_batch_id`: A newly generated UUID.
   - `ingest_date`: Current date string formatted as `YYYY-MM-DD`.
6. Group the dataframe by `entity_type` (customers, orders) and `ingest_date`.
7. Append the data to the corresponding local paths:
   - Customers: `data/bronze/customers/ingest_date={date}/`
   - Orders: `data/bronze/orders/ingest_date={date}/`
8. Save the data in Parquet format using the `fastparquet` or `pyarrow` engine.
9. Create the archive directory `data/cdc_archive/` if it does not exist.
10. Move all processed JSON files from `data/cdc_buffer/` to `data/cdc_archive/`.

# Validation Checklist
- [ ] Run `python scripts/ingest_bronze.py` and verify it processes files from `data/cdc_buffer/`.
- [ ] Verify that directory structures are created like `data/bronze/customers/ingest_date=YYYY-MM-DD/`.
- [ ] Inspect a written Parquet file using a python tool (e.g., pandas `read_parquet`) and assert that `_bronze_ingested_at` and `_bronze_batch_id` are populated.
- [ ] Verify the source JSON files have been completely moved to `data/cdc_archive/`.
- [ ] Run the script again with an empty buffer and ensure it exits without errors or overwriting existing parquet data.

# Rollback Plan
- Delete the newly created Parquet directories in `data/bronze/`.
- Move the processed JSON files back from `data/cdc_archive/` to `data/cdc_buffer/`.
- Discard the changes to `scripts/ingest_bronze.py` by running `git checkout -- scripts/ingest_bronze.py`.

# Expected Git Commit Message
feat(bronze): implement raw append-only ingestion and schema capture
