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

def slack_failure_callback(context):
    """
    Mock Slack failure callback function. Writes failed task alerts to logs/slack_alerts.log.
    """
    import os
    from datetime import datetime
    
    # Extract details from execution context
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id if task_instance else 'unknown_task'
    dag_id = task_instance.dag_id if task_instance else 'unknown_dag'
    run_id = context.get('run_id', 'unknown_run')
    exception = context.get('exception', 'No exception trace')
    
    log_dir = "/opt/airflow/logs"
    if not os.path.exists(log_dir):
        log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    
    alert_path = os.path.join(log_dir, "slack_alerts.log")
    alert_msg = (
        f"[{datetime.now().isoformat()}] [SLACK ALERT] 🚨 Pipeline Failure Detected!\n"
        f"  DAG: {dag_id}\n"
        f"  Task: {task_id}\n"
        f"  Run ID: {run_id}\n"
        f"  Error Exception: {exception}\n"
        f"  Action Required: Check task logs for details.\n"
        "--------------------------------------------------\n"
    )
    with open(alert_path, 'a') as f:
        f.write(alert_msg)
    print(f"Mock Slack Alert written to: {alert_path}")

# Default task arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': slack_failure_callback,
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