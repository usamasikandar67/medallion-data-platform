# Revision Log: Milestone 1 (Operational Database Simulation & CDC)

## Version History

| Version | Date | Author | Changes |
| :--- | :--- | :--- | :--- |
| v1.0.1 | 2026-07-08 | Principal Data Engineering Architect | Document trigger-based audit logging assumption to satisfy UPDATE_BEFORE and DELETE capture. |
| v1.0.0 | 2026-07-08 | Principal Data Engineering Architect | Initial specification and design plan for source simulation and CDC capture. |

---

## Design Decisions
- **Decision 1 (Change Feed Capture Strategy)**: Decided to capture both `UPDATE_BEFORE` and `UPDATE_AFTER` logs for customer changes. This allows the Silver layer to compute precise deltas for attributes if needed, though only `UPDATE_AFTER` is strictly required to perform basic SCD Type 2 MERGE updates.
- **Decision 2 (Engine Language)**: Selected Python for the simulation scripts to maximize interoperability with typical PySpark and dbt-python analytical environments.
- **Decision 3 (Trigger-Based CDC Logging)**: To capture `UPDATE_BEFORE` and actual SQL `DELETE` operations without soft-deletes, we employ database triggers on the SQLite `customers` and `orders` tables. These triggers append all mutations to a conformed `cdc_event_log` table inside the SQLite source database. The CDC Extractor then queries this log table based on the last processed sequence ID or timestamp. This ensures "exactly-once" delivery, captures historical updates, and tracks hard deletes.

---

## Open Questions
- **Q1**: Do we need to simulate cascading deletes? (e.g., if a customer is deleted, are all their orders deleted in the CDC feed?)
  - *Current Resolution*: No. Orders will remain as orphaned/unchanged records, mimicking realistic transactional databases where soft deletes or constraint enforcement are handled separately.

---

## Review Notes
- **Linter / Schema constraints**: Ensure the output JSON CDC feed has a strict, documented schema definition so that the Bronze layer ingestion script doesn't require complex parser updates if field formats change.
- **Verification Review (2026-07-08)**: 
  - Database schema initialized with WAL journaling mode.
  - SQLite Triggers successfully track customer and order creations, updates (both `UPDATE_BEFORE` and `UPDATE_AFTER`), and deletes.
  - Simulator runs cleanly, populating operations and printing real-time console messages.
  - CDC Extractor reads incrementing sequence IDs and writes well-formed, isolated JSON batch feeds under `data/cdc_buffer/` with filenames like `cdc_batch_{timestamp}.json`.
  - State file `data/cdc_state.json` successfully logs processed sequence markers.
