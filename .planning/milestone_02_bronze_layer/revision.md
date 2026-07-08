# Revision Log: Milestone 2 (Bronze Layer Ingestion)

## Version History

| Version | Date | Author | Changes |
| :--- | :--- | :--- | :--- |
| v1.0.1 | 2026-07-08 | Principal Data Engineering Architect | Implemented ingestion script and set up python virtual environment dependencies. |
| v1.0.0 | 2026-07-08 | Principal Data Engineering Architect | Initial layout of the Bronze Layer append-only storage and partitioning strategy. |

---

## Design Decisions
- **Decision 1 (Partition Granularity)**: Decided to partition by ingestion date (`ingest_date`) rather than transactional date (`created_at`). This ensures that late-arriving transactional data is written to the current date's partition folder, avoiding historical partition rewrites and keeping the pipeline simple and efficient.
- **Decision 2 (Metadata Schema)**: Formatted `_bronze_ingested_at` to use ISO 8601 UTC format (`YYYY-MM-DDTHH:MM:SSZ`) to avoid time-zone conversion confusion downstream.

---

## Open Questions
- **Q1**: What happens if the archiver fails to move files after they are written to Parquet?
  - *Current Resolution*: Wrap the parquet write and file moves in a single Python `try...except` block. If the write fails, files remain in the buffer. If the archive move fails, alert is sent, and manual inspection is required to prevent duplicate loads next batch.

---

## Review Notes
- **File System Permissions**: In a production environment, the runner needs permissions to write to target storage folders and delete/move files from raw buffers.
- **Verification Review (2026-07-08)**:
  - Ingestion pipeline script `scripts/ingest_bronze.py` implemented and verified.
  - Successfully read JSON CDC payloads from `data/cdc_buffer/`, conformed schema by entity type, enriched with `_bronze_ingested_at` and `_bronze_batch_id`, and wrote partitioned Parquet structures.
  - Processed JSON feed files successfully archived to `data/cdc_archive/`.
  - Confirmed empty buffer runs terminate gracefully without rewriting or erroring out.
