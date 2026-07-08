# Milestone 7 Spec: Deployment & CI/CD Orchestration

## Objective
Establish the deployment framework, CI/CD automation pipelines, and task orchestration DAG (Directed Acyclic Graph) configurations to automate the execution of the entire data pipeline (Simulation -> CDC -> Bronze -> Silver -> Gold -> dbt -> DQ -> Monitoring) in a production environment.

---

## Business Problem
Running data engineering pipelines manually is not sustainable for enterprise operations. In production, steps must execute automatically on schedules or triggers, handle upstream dependencies, notify engineers of failures, and support zero-downtime upgrades. Furthermore, manual configuration deployments introduce drift and bugs. Orchestrating task sequences with workflow software and setting up automated CI/CD validation gates are essential to guarantee deployment stability and platform reliability.

---

## Functional Requirements
1. **Pipeline Orchestration (DAG)**:
   - Configure a workflow manager (e.g. Apache Airflow, Prefect, or Dagster).
   - Define a Directed Acyclic Graph (DAG) detailing the scheduling and task dependencies:
     - Task 1: Generate operational DB simulated modifications.
     - Task 2: Trigger CDC extraction batch.
     - Task 3: Ingest change logs to Bronze (depends on Task 2).
     - Task 4: Execute dbt incremental models (depends on Task 3).
     - Task 5: Execute dbt data quality tests (depends on Task 4).
     - Task 6: Refresh monitoring and export dashboard assets (depends on Task 5).
   - Configure automatic task retries (e.g., retry 3 times with 1-minute exponential backoff) and email/alert triggers.
2. **Infrastructure-as-Code & Containerization**:
   - Write a `Dockerfile` specifying execution environments, Python runtimes, database libraries, and dbt.
   - Configure a `docker-compose.yml` defining the local sandbox execution environment (Source database, Lakehouse storage volume, Orchestrator service).
3. **CI/CD Pipeline Configurations**:
   - Write a configuration file (e.g. GitHub Actions `.github/workflows/deploy.yml` or GitLab CI) to automate code validation:
     - Trigger on code commits or pull requests to main branch.
     - Spin up database environments, build dependencies, run code linters, execute dbt test commands, and confirm build success.

---

## Non-functional Requirements
- **High Availability**: The orchestrator must keep task state logs in a persistent metadata database to support recovery if services restart.
- **Security**: Database passwords, cloud bucket keys, and API tokens must be injected as secure environment variables, never hardcoded.
- **Scalability**: Containers must support resource limits to prevent memory leaks from starving other system services.

---

## Inputs
- **Platform Code**: All scripts, sql views, and dbt models built in prior milestones.
- **Environment variables**: Configurations specifying credentials and target directory paths.

---

## Outputs
- **Deployment Configs**: Docker assets and environment variables.
- **Orchestration Definition**: Airflow DAG python script or Prefect flow.
- **CI/CD Files**: Automated test pipeline YAML configuration.

---

## Architecture Decisions
- **Orchestrator Selection**: Apache Airflow (or local docker-compose script running the orchestration loop) is chosen for scheduling. Airflow's Python-based DAG definition integrates seamlessly with the python transformation scripts and dbt CLI.
- **Isolated Runners**: Docker containers will wrap execution tasks. This isolates Python library versions (like dbt vs. sqlite database connectors) and allows clean port forwarding for dashboards.

---

## Dependencies
- Language runtimes: Python / Docker / YAML.
- Workflow engine: Apache Airflow (or lightweight cron orchestrator).
- Deployment repository: GitHub/GitLab.

---

## Acceptance Criteria
- [ ] Docker compose starts the simulator, warehouse, and orchestrator containers.
- [ ] The orchestrator schedules and runs the pipeline tasks in the exact defined sequence.
- [ ] Task execution logs are captured by the orchestrator.
- [ ] Git commit to a pull request triggers the CI/CD pipeline, building and testing the project.
- [ ] Secrets and database credentials are fully resolved from environment variables.

---

## Risks
- **Orchestration Resource Constraints**: Running multiple micro-batches concurrently in low-memory container environments can trigger Out-of-Memory (OOM) kills.
  - *Mitigation*: Limit orchestrator concurrent task instances to 1-2 threads, and run vacuum/maintenance commands on databases.

---

## Deliverables
1. `Dockerfile` and `docker-compose.yml`.
2. Workflow DAG Script (`orchestration/pipeline_dag.py`).
3. Automated CI/CD workflow YAML (`.github/workflows/ci.yml`).

---

## Future Improvements
- Promoted to cloud orchestration platforms (e.g. Managed Workflows for Apache Airflow on AWS or astronomer.io) and set up Kubernetes executors for dynamic auto-scaling of ingestion tasks.
