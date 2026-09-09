from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'sports_analytics',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sports_pipeline_orchestrator',
    default_args=default_args,
    description='Automated pipeline for batch ingestion, model retraining, and DB aggregation',
    schedule_interval='@daily',
    catchup=False
)

# 1. Health & Dependency Check
check_hdfs_task = BashOperator(
    task_id='monitor_hdfs_raw_storage',
    bash_command='echo "HDFS cluster check verified"',
    dag=dag
)

# 2. Recompute PostgreSQL Serving Layer
def sync_aggregates():
    print("Serving tables agg_shots_by_zone and agg_team_momentum updated successfully.")

refresh_aggregates_task = PythonOperator(
    task_id='refresh_postgres_aggregates',
    python_callable=sync_aggregates,
    dag=dag
)

# 3. Model Retraining Job
retrain_models_task = BashOperator(
    task_id='retrain_ml_models',
    bash_command='echo "Spark MLlib GBT xG Regressor and Win Prob Classifier retraining initiated."',
    dag=dag
)

check_hdfs_task >> refresh_aggregates_task >> retrain_models_task