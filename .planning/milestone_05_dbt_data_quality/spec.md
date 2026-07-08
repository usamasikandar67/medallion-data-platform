# Milestone 5 Spec: dbt & Data Quality

## Objective
Migrate transformation execution steps (Bronze -> Silver -> Gold) into a dbt (data build tool) project structure. Configure data lineage graphs, write documentation, and enforce data quality gates using dbt tests or third-party statistical quality checking libraries (e.g., Soda Core, Great Expectations).

---

## Business Problem
Without structured modeling and transformation tools, pipelines quickly become hard-coded, error-prone scripts with hidden dependencies. Understanding the data lineage (how a change in Bronze impacts a KPI in Gold) is difficult. Furthermore, bad data (e.g., negative transaction amounts, missing keys, invalid format states) can pollute the warehouse silently. Applying dbt ensures centralized logic, visual lineage documentation, and automated tests checking rules at every layer before analytical users query the data.

---

## Functional Requirements
1. **dbt Project Organization**:
   - Establish a standard dbt repository (with `dbt_project.yml`, `models/`, `tests/`, `seeds/`).
   - Configure a dbt connection profile (`profiles.yml`) linking to the warehouse.
2. **Medallion Model Migrations**:
   - **Sources**: Define external Bronze paths/tables in a `sources.yml` file.
   - **Staging / Silver Models**: Write SQL models with dynamic `{{ config(materialized='incremental') }}` or table configurations capturing Bronze updates.
   - **Gold Models**: Write SQL models creating Star Schema tables (`dim_customers`, `fact_orders`), utilizing dbt `ref()` functions to establish dependencies.
3. **Data Quality Tests**:
   - Apply standard dbt constraints in schema configurations (`schema.yml`):
     - `unique` and `not_null` assertions on all primary keys (e.g. `customer_sk`, `order_id`).
     - `accepted_values` on conformed columns (e.g. `order_status` in 'Pending', 'Shipped', 'Delivered', 'Cancelled').
     - `relationships` foreign key validation between `fact_orders.customer_sk` and `dim_customers.customer_sk`.
   - Write custom SQL singular tests for advanced criteria (e.g., checking that `order_amount` is always non-negative).
4. **Lineage & Documentation**:
   - Write docstrings for all models and columns.
   - Generate dbt docs containing automated lineage charts showing full flow from Source to Gold.

---

## Non-functional Requirements
- **Execution Validation**: Data quality tests must run automatically immediately after models build (`dbt build` or `dbt test`).
- **Fail-Fast Configuration**: Enforce `--fail-fast` options so that if a staging model test fails, downstream Gold models are not processed.
- **Portability**: Profile configurations must utilize environment variables to support seamless environment promotion (e.g. dev, staging, prod) without code changes.

---

## Inputs
- **Raw Tables**: Bronze layer data folders.
- **Conformed Tables**: Silver layer schema structures.
- **SQL Logics**: Transformation logic written in Milestones 3 and 4.

---

## Outputs
- **dbt Project Artifacts**:
  - `dbt_project.yml` and `profiles.yml`.
  - `models/staging/` and `models/marts/` SQL files.
  - `target/manifest.json` and compiled SQL queries.
  - `target/index.html` (dbt lineage documentation).

---

## Architecture Decisions
- **Incremental Materialization**: Silver models will be materialized using `incremental` strategies with unique key configurations. Gold fact tables will also be incremental, whereas Gold dimensions will be table-materialized to rebuild full history updates cleanly.
- **Data Quality Framework**: Combine dbt native tests for structural assertions (uniqueness, referential integrity) with a custom hook or validation script (e.g., Soda Core) for statistical validations (e.g., schema drift, row count anomalies).

---

## Dependencies
- Language runtimes: Python / Node.js.
- Execution libraries: `dbt-core` and adapter libraries (e.g., `dbt-sqlite`, `dbt-postgres`, or `dbt-snowflake`).
- Validation tools: Soda Core (optional/recommended).

---

## Acceptance Criteria
- [ ] Running `dbt debug` successfully connects to the warehouse.
- [ ] Running `dbt run` builds staging, Silver, and Gold structures in correct execution order based on references.
- [ ] Running `dbt test` executes all schema tests and custom SQL assertions.
- [ ] If a record violates a `not_null` or `unique` constraint in Staging, `dbt test` returns a failure state and exits with a non-zero code.
- [ ] The generated `dbt docs` contains complete documentation and lineage nodes for all models.

---

## Risks
- **Concurrency Conflicts**: Simultaneous dbt runs during source updates might lock the database file.
  - *Mitigation*: Ensure dbt runs with a connection limit of 1 in the target profile config for SQLite testing.

---

## Deliverables
1. Complete dbt project code directory (`transformations/`).
2. Model definition yaml files (`schema.yml`, `sources.yml`).
3. Custom SQL singular test scripts.

---

## Future Improvements
- Integrate dbt Cloud or setup automatic doc deployment to static hosting (like GitHub Pages or AWS S3) for engineering and business visibility.
