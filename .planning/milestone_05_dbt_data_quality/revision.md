# Revision Log: Milestone 5 (dbt & Data Quality)

## Version History

| Version | Date | Author | Changes |
| :--- | :--- | :--- | :--- |
| v1.0.1 | 2026-07-08 | Principal Data Engineering Architect | Initialized dbt transformations repository, migrated models, mapped SQLite profiles, and configured data quality tests. |
| v1.0.0 | 2026-07-08 | Principal Data Engineering Architect | Initial spec-driven layout for dbt structure integration and data quality gates. |

---

## Design Decisions
- **Decision 1 (Incremental Strategy)**: Decided to use dbt's native `incremental` materialization for Silver tables using the `merge` incremental strategy (supported by most adapters). This ensures we leverage dbt's pre-configured merge queries rather than maintaining custom Python scripts.
- **Decision 2 (Environment Separation)**: Hardcoded references are disallowed. Profile configurations will utilize `{{ env_var('DB_PATH') }}` to dynamically swap databases between test and production runs.

---

## Open Questions
- **Q1**: Should we run data quality tests on Bronze tables?
  - *Current Resolution*: Yes, but as warning-only tests (`severity: warn`). If Bronze data has schema drift or formatting issues, we want to know, but we should not halt ingestion since Bronze is the primary historical archive.

---

## Review Notes
- **dbt Documentation**: Running `dbt docs generate` must be part of the final build pipeline step to ensure visual representation is always up-to-date with code modifications.
- **Verification Review (2026-07-08)**:
  - Standard dbt project initialized under `transformations/` directory.
  - Setup local SQLite credentials configuration profile `profiles.yml` with `schemas_and_paths` and `schema_directory` properties.
  - Migrated Medallion models: staging views (`stg_customers`, `stg_orders`), conformed tables (`silver_customers` with SQL-based window lead partitions for SCD2, `silver_orders` deduplicated chronologically), and analytical layers (`dim_customers` with Unknown Customer default union, `fact_orders` resolving surrogate keys).
  - Executed `dbt debug` confirming database connection test is OK.
  - Executed `dbt build` successfully running 6 models and passing all 20 tests.
  - Verified fail-fast data quality gates: artificially inserting a negative `order_amount` caused test `assert_order_amount_non_negative` to fail as expected, halting build propagation.
  - Generated dbt documentation catalog files (`catalog.json` and lineage maps) successfully.
