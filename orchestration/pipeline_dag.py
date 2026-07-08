from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

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
        cwd='/opt/airflow'
    )

    # 2. Bronze Ingestion task
    run_bronze_ingestion = BashOperator(
        task_id='run_bronze_ingestion',
        bash_command='python3 /opt/airflow/scripts/ingest_bronze.py',
        cwd='/opt/airflow'
    )

    # 3. Bronze to SQLite load helper
    run_bronze_to_sqlite = BashOperator(
        task_id='run_bronze_to_sqlite',
        bash_command='python3 /opt/airflow/scripts/load_bronze_to_sqlite.py',
        cwd='/opt/airflow'
    )

    # 4. dbt models execution
    run_dbt_models = BashOperator(
        task_id='run_dbt_models',
        bash_command='dbt run --profiles-dir .',
        cwd='/opt/airflow/transformations'
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
        cwd='/opt/airflow'
    )

    # Sequencing and dependencies mapping
    run_cdc_extraction >> run_bronze_ingestion >> run_bronze_to_sqlite >> run_dbt_models >> run_dbt_tests >> generate_dashboard_data
