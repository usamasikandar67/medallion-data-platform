# Revision Log: Milestone 7 (Deployment & CI/CD Orchestration)

## Version History

| Version | Date | Author | Changes |
| :--- | :--- | :--- | :--- |
| v1.0.1 | 2026-07-08 | Principal Data Engineering Architect | Implemented Dockerfile, docker-compose.yml services configuration, Airflow DAG, and GitHub Actions CI workflow script. |
| v1.0.0 | 2026-07-08 | Principal Data Engineering Architect | Initial spec layout for deployment containerization, DAG workflow scheduling, and CI/CD pipelines. |

---

## Design Decisions
- **Decision 1 (Orchestration Engine)**: Decided to use a lightweight Python-based runner scheduler or Docker container with Apache Airflow local executor. This provides enterprise-grade orchestration capabilities without requiring heavy Kubernetes administration locally.
- **Decision 2 (Environment variables)**: Environment keys like `DBT_PROFILES_DIR` and `WAREHOUSE_DB_URL` are strictly mandated to be resolved via a `.env` file read during Docker container spin-up.

---

## Open Questions
- **Q1**: How do we handle database backups in containerized environments?
  - *Current Resolution*: Configure a volume mount in `docker-compose.yml` mapped to the local host's `data/` folder. This ensures that even if containers are destroyed, database state remains preserved on the host file system.

---

## Review Notes
- **Docker Compose Ports**: Ensure default dashboard port mappings (e.g. port 8080 or 80) do not conflict with local application servers. Use customizable port variables inside `.env`.
- **Verification Review (2026-07-08)**:
  - Docker containerization verified with `Dockerfile` using `apache/airflow:2.9.2-python3.11` as the base image.
  - Multi-service deployment structured in `docker-compose.yml` including metadata database (`postgres`), workflow scheduler (`airflow-scheduler`), web console UI (`airflow-webserver`), and database workload simulator (`simulator`).
  - Airflow workflow declared in `orchestration/pipeline_dag.py` defining task sequencing (`CDC Extraction` -> `Bronze Ingestion` -> `Bronze to SQLite` -> `dbt run` -> `dbt test` -> `Generate Dashboard Data`).
  - Automated pull request checks configured in `.github/workflows/ci.yml` running a mock timeout simulation run and asserting all dbt quality gates pass.
  - Verified local build and execution flow. All tasks compile cleanly.
