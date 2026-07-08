# Goal
Configure deployment containerization files, write Airflow orchestration tasks, and formulate the automated CI/CD pipeline script to establish production readiness.

# Implementation Steps
1. Create a `Dockerfile` in the root workspace defining the run environment with Python 3.10, installing dependencies: `pandas`, `fastparquet`, `dbt-sqlite`, and `apache-airflow`.
2. Create a `docker-compose.yml` specifying:
   - Service `simulator`: Runs `python scripts/db_simulator.py`.
   - Service `orchestrator`: Mounts local directories and runs Airflow webserver/scheduler.
3. Create orchestration tasks file `orchestration/pipeline_dag.py` defining:
   - Task `run_cdc_extraction`: Executes `python scripts/cdc_extractor.py`.
   - Task `run_bronze_ingestion`: Executes `python scripts/ingest_bronze.py`.
   - Task `run_dbt_models`: Executes `dbt run --profiles-dir .` (or runs load scripts).
   - Task `run_dbt_tests`: Executes `dbt test --profiles-dir .`.
   - Task `generate_dashboard_data`: Runs `python scripts/generate_dashboard_data.py`.
   - Setup dependencies: `run_cdc_extraction >> run_bronze_ingestion >> run_dbt_models >> run_dbt_tests >> generate_dashboard_data`.
4. Create `.github/workflows/ci.yml` defining workflow steps:
   - On pull request to main.
   - Run linter checks (flake8/black).
   - Setup sandbox database.
   - Run dbt run and dbt test.
   - Assert all tests pass before allowing PR merges.

# Validation Checklist
- [ ] Build the container using `docker build -t cdc-platform .` and confirm build success.
- [ ] Run `docker-compose up -d` and verify all containers (simulator, orchestrator) start and enter active running states.
- [ ] Access Airflow web UI (e.g. `localhost:8080`), enable `pipeline_dag`, and trigger manually.
- [ ] Confirm in the DAG run logs that all 5 tasks complete successfully in order.
- [ ] Verify that new data is generated in the warehouse and exported dashboard json.
- [ ] Run CI/CD steps locally using a runner tool (like `act`) to confirm that linters and compilation tests execute.

# Rollback Plan
- Stop docker-compose services using `docker-compose down -v`.
- Remove docker images from local registry using `docker rmi cdc-platform`.
- Delete `.github/workflows/ci.yml` and `orchestration/pipeline_dag.py` configuration files.
- Revert environment configurations.

# Expected Git Commit Message
feat(deploy): implement orchestration pipelines and cicd workflows
