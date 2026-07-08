# Revision Log: Milestone 4 (Gold Dimensional Model)

## Version History

| Version | Date | Author | Changes |
| :--- | :--- | :--- | :--- |
| v1.0.1 | 2026-07-08 | Principal Data Engineering Architect | Implemented Gold Star Schema tables, Date dimension generator, and historical surrogate keys loader. |
| v1.0.0 | 2026-07-08 | Principal Data Engineering Architect | Initial layout of the Gold Dimensional Model (Star Schema) specifications. |

---

## Design Decisions
- **Decision 1 (Time-slice matching)**: Decided to map orders to customers using a `BETWEEN` operator: `orders.created_at BETWEEN customers.valid_from AND customers.valid_to`. To make this robust, the low end of time comparison checks is inclusive, while the high end is exclusive (or handles the high date string safely).
- **Decision 2 (Date Dimension Generation)**: Chose to pre-generate a static `dim_date` CSV covering years 2025 to 2030, rather than calculating it on-the-fly, to guarantee maximum read performance.

---

## Open Questions
- **Q1**: Do we need to support multiple timezones in the Date dimension?
  - *Current Resolution*: No. The warehouse will operate strictly in UTC. All timezone conversions must be performed on the dashboard layer or client visualization application.

---

## Review Notes
- **ForeignKey constraints**: While SQLite does not strictly enforce foreign key relationships across files unless configured, standard SQL constraints will be written into DDL files for compatibility with PostgreSQL and Snowflake.
- **Verification Review (2026-07-08)**:
  - Table definitions script `scripts/create_gold_tables.sql` successfully initialized Gold schemas in `data/warehouse.db`.
  - Date Dimension generator script `scripts/generate_dim_date.py` successfully loaded 2191 rows (covering 2025-01-01 to 2030-12-31) with appropriate `date_key`, calendar, day, month, year, and quarter fields.
  - Analytical loading script `scripts/load_gold.py` successfully populated dimensions, handles default Unknown Row `customer_sk = -1`, and joins facts using point-in-time comparisons (SCD Type 2).
  - Validation queries checked and verified that no orphaned rows are present in `fact_orders`.
  - Confirmed orders placed by updated customers link to the exact surrogate key active at the specific order placement datetime.
