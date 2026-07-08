# Enterprise Data Engineering Platform: Roadmap

This document outlines the chronological execution path for implementing the data engineering platform. Every milestone is a hard dependency for the subsequent milestone.

| Milestone | Title | Est. Effort | Core Dependencies | Expected Commit Message Template |
| :--- | :--- | :--- | :--- | :--- |
| **M1** | Operational Database Simulation & CDC | 3 Days | None | `feat(cdc): implement source simulation and delta change data feed` |
| **M2** | Bronze Layer Ingestion & Partitioning | 2 Days | M1 | `feat(bronze): implement raw append-only ingestion and schema capture` |
| **M3** | Silver Layer MERGE-based SCD Type 2 | 4 Days | M2 | `feat(silver): implement staging and merge-based scd type 2 updates` |
| **M4** | Gold Dimensional Star Schema Model | 3 Days | M3 | `feat(gold): implement fact and dimension analytical model` |
| **M5** | dbt Transformation & Data Quality | 3 Days | M4 | `feat(dbt): integrate dbt models and configure data quality tests` |
| **M6** | Monitoring Observability & Dashboard | 2 Days | M5 | `feat(monitor): implement execution logging and dashboard queries` |
| **M7** | Production Deployment & CI/CD | 3 Days | M6 | `feat(deploy): implement orchestration pipelines and cicd workflows` |

---

## Detailed Milestone Schedule

### Milestone 1: Operational Database Simulation & CDC
* **Focus**: Setting up the source database environment and simulating real-time OLTP workloads (Inserts, Updates, Deletes). Capturing those changes as they occur.
* **Effort**: 3 days.
* **Dependencies**: None.

### Milestone 2: Bronze Layer (Raw Storage)
* **Focus**: Raw ingestion pipelines that append incoming change feeds into partitions while keeping full history.
* **Effort**: 2 days.
* **Dependencies**: Milestone 1.

### Milestone 3: Silver Layer (MERGE-based SCD Type 2)
* **Focus**: Cleaning raw data, handling late-arriving dimensions, deduplicating, and running MERGE statements to perform SCD Type 2 history keeping (`valid_from`, `valid_to`, `is_current`).
* **Effort**: 4 days.
* **Dependencies**: Milestone 2.

### Milestone 4: Gold Layer (Dimensional Analytical Model)
* **Focus**: Denormalized dimension and fact models structured as a Star Schema, optimized for reporting.
* **Effort**: 3 days.
* **Dependencies**: Milestone 3.

### Milestone 5: dbt & Data Quality
* **Focus**: Migration of transformation logic into dbt, adding tests, generating lineage maps, and setting up automated quality gates.
* **Effort**: 3 days.
* **Dependencies**: Milestone 4.

### Milestone 6: Monitoring & Dashboard
* **Focus**: Logging ingestion and MERGE execution duration, row counts, and data quality scores. Querying Gold for a simulated dashboard.
* **Effort**: 2 days.
* **Dependencies**: Milestone 5.

### Milestone 7: Deployment & CI/CD Orchestration
* **Focus**: Continuous deployment workflows, Infrastructure-as-code, and orchestrating the operational runs (e.g. Airflow/Prefect/Dagster).
* **Effort**: 3 days.
* **Dependencies**: Milestone 6.
