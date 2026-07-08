# Medallion Data Platform: Ingestion, Orchestration, and Data Quality

An enterprise-grade, spec-driven Medallion data engineering platform implementing real-time Change Data Capture (CDC), conformed Slowly Changing Dimensions (SCD Type 2), Kimball Star Schema modeling, dbt validation checkpoints, and local container orchestration.

---

## 🏛️ System Architecture

The platform processes data chronologically through three conformed lakehouse layers:

```
[OLTP Source DB] (Simulated Transactions)
       │
       ▼ (Database Triggers)
[cdc_event_log]
       │
       ▼ (CDC Extractor JSON files)
[Bronze Area] (Raw Append-Only Parquet)
       │
       ▼ (Staging Views & dbt cleanings)
[Silver Area] (Conformed SCD Type 2 History)
       │
       ▼ (Point-in-Time Temporal Joins)
[Gold Area] (Kimball Star Schema Fact & Dimensions)
       │
       ▼ (Analytics aggregates views)
[UI Observability Dashboard] (Sleek Chart.js Visualizations)
```

---

## 🚀 Key Features

1. **Transaction Simulation & CDC**: Logs creations, updates, and deletes automatically using SQLite `AFTER` triggers recording to a dedicated `cdc_event_log`.
2. **Medallion Data Warehouse**: Partitioned Bronze Parquet files, Silver Slowly Changing Dimensions (SCD Type 2) tracking validity periods, and Gold Star Schema models handling point-in-time order facts joins.
3. **dbt & Data Quality Gates**: Over 14 test checks (uniqueness, nullable keys, foreign reference constraints, status checks) and a custom test validating order prices are positive.
4. **Monitoring & Alerts**: Track run execution durations, success/failure status codes, and stack traces inside the `monitor_pipeline_runs` table.
5. **Interactive UI Dashboard**: Render revenue statistics, active client numbers, shipping status spreads, and execution logs visually via a modern HTML template using Chart.js.
6. **Containerized Orchestration**: Automated task dependencies scheduled via an **Apache Airflow DAG** inside Docker Compose.
7. **CI/CD Workflows**: Fully validated pull request workflows running test iterations on GitHub Actions.

---

## 🛠️ Quickstart Guide

### Option 1: Deploying via Docker (Recommended)
Make sure Docker Desktop is running on your system, then boot the platform using:

```bash
# 1. Build the container images
docker build -t cdc-platform .

# 2. Spin up Airflow, Postgres, and the simulator
docker compose up -d
```

#### Endpoints:
- **Airflow Console UI**: Go to [http://localhost:8080](http://localhost:8080) (Credentials: `admin` / `admin`).
- **Observability Dashboard**: Open the local dashboard in your browser: `dashboard/index.html`.

### Option 2: Running Locally (Local virtual environment)
Setup your local Python environment:

```bash
# 1. Initialize virtual environment and install packages
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt # or pip install pandas pyarrow fastparquet dbt-core dbt-sqlite

# 2. Run the processing scripts sequentially
python3 scripts/load_bronze_to_sqlite.py
python3 scripts/generate_dim_date.py
cd transformations
dbt build --profiles-dir .
```

---

## 📁 Repository Structure
- `.github/workflows/ci.yml`: CI validation actions.
- `.planning/`: Detailed project milestone specifications.
- `dashboard/`: Visual rendering and JS data exporters.
- `orchestration/`: Airflow DAG configuration.
- `scripts/`: Simulator, extraction, loading, and logging scripts.
- `transformations/`: Staging, conformed, and analytical dbt SQL models.
- `Dockerfile` & `docker-compose.yml`: Multi-container deployments.
