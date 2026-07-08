# Revision Log: Milestone 3 (Silver Layer SCD Type 2)

## Version History

| Version | Date | Author | Changes |
| :--- | :--- | :--- | :--- |
| v1.0.1 | 2026-07-08 | Principal Data Engineering Architect | Implemented Silver loader table creation DDL, staging cleansing, and MERGE SCD Type 2 transactional code. |
| v1.0.0 | 2026-07-08 | Principal Data Engineering Architect | Initial spec for Silver layer cleansing and MERGE-based SCD Type 2 tracking. |

---

## Design Decisions
- **Decision 1 (Surrogate Key generation)**: Selected MD5 hashing of `customer_id` concatenated with `valid_from` to generate a 32-character hexadecimal string surrogate key (`customer_sk`). This avoids the need for an auto-increment sequence which is difficult to manage idempotently in distributed environments.
- **Decision 2 (High Date Standard)**: Standardized `9999-12-31 23:59:59` as the default high timestamp for active records to facilitate simple range queries using standard SQL `BETWEEN` operators.

---

## Open Questions
- **Q1**: How should we handle updates to non-tracked columns (columns that shouldn't trigger a new SCD Type 2 row, such as typo corrections in first names)?
  - *Current Resolution*: For this implementation, all customer columns (except metadata) will trigger SCD Type 2 tracking. Future optimizations can implement a column selection list to partition Type 1 (overwrite) vs. Type 2 (history tracking) updates.

---

## Review Notes
- **Index Optimization**: The silver customer table must have index configurations on `customer_id` and `is_current` to keep updates and MERGE operations fast as history accumulates.
- **Verification Review (2026-07-08)**:
  - SQL DDL script `scripts/create_silver_tables.sql` successfully initialized schemas in `data/warehouse.db`.
  - Staging cleansing, null key checking, and data-type casting successfully conformed incoming records.
  - Invalid customer rows (e.g. order records with `customer_id = NaN` triggered by cascading delete events) are successfully captured and written to JSON files inside `data/quarantine/`.
  - Incremental MERGE SCD Type 2 logic updates and closes superseded customer history rows, writing new active versions (`valid_to = 9999-12-31 23:59:59` and `is_current = 1`) with unique MD5 surrogate keys.
  - Upserts to `silver_orders` verified.
  - Full process wrapped inside database transaction locks for ACID safety.
  - Load idempotence verified on sequential runs.
