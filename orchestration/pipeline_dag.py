from datetime import datetime, timedelta
# pyrefly: ignore [missing-import]
from airflow import DAG
# pyrefly: ignore [missing-import]
from airflow.operators.bash import BashOperator
# pyrefly: ignore [missing-import]
from airflow.datasets import Dataset

# Declare conformed Datasets for lineage tracking
cdc_buffer = Dataset("file:///opt/airflow/data/cdc_buffer")
bronze_parquet = Dataset("file:///opt/airflow/data/bronze")
warehouse_db = Dataset("file:///opt/airflow/data/warehouse.db")
dashboard_stats = Dataset("file:///opt/airflow/data/dashboard_stats.json")

# Default task arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'medallion_pipeline_dag',
    default_args=default_args,
    description='Medallion Data Platform Processing DAG',
    schedule_interval='*/30 * * * *', # Runs every 30 minutes
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    # 1. CDC Extraction task
    run_cdc_extraction = BashOperator(
        task_id='run_cdc_extraction',
        bash_command='python3 /opt/airflow/scripts/cdc_extractor.py',
        cwd='/opt/airflow',
        outlets=[cdc_buffer]
    )

    # 2. Bronze Ingestion task
    run_bronze_ingestion = BashOperator(
        task_id='run_bronze_ingestion',
        bash_command='python3 /opt/airflow/scripts/ingest_bronze.py',
        cwd='/opt/airflow',
        outlets=[bronze_parquet]
    )

    # 3. Bronze to SQLite load helper
    run_bronze_to_sqlite = BashOperator(
        task_id='run_bronze_to_sqlite',
        bash_command='python3 /opt/airflow/scripts/load_bronze_to_sqlite.py',
        cwd='/opt/airflow',
        outlets=[warehouse_db]
    )

    # 4. dbt models execution
    run_dbt_models = BashOperator(
        task_id='run_dbt_models',
        bash_command='dbt run --profiles-dir .',
        cwd='/opt/airflow/transformations',
        outlets=[warehouse_db]
    )

    # 5. dbt quality checks/tests
    run_dbt_tests = BashOperator(
        task_id='run_dbt_tests',
        bash_command='dbt test --profiles-dir .',
        cwd='/opt/airflow/transformations'
    )

    # 6. Generate dashboard exports
    generate_dashboard_data = BashOperator(
        task_id='generate_dashboard_data',
        bash_command='python3 /opt/airflow/scripts/generate_dashboard_data.py',
        cwd='/opt/airflow',
        outlets=[dashboard_stats]
    )

    # Sequencing and dependencies mapping
    run_cdc_extraction >> run_bronze_ingestion >> run_bronze_to_sqlite >> run_dbt_models >> run_dbt_tests >> generate_dashboard_data
