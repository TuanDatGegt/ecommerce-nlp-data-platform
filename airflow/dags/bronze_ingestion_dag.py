#airflow/dags/bronze_ingestion_dag.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipeline.bronze.orchestration.bronze_pipeline import run_pipeline

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bronze_ingestion_dag",
    description=(
        "Bronze Layer Ingestion Pipeline "
        "for Amazon Reviews Dataset"
    ),

    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["bronze", "etl", 'minio', 'parquet', 'amazon_reviews']
) as dag:
    
    bronze_ingestion_task = PythonOperator(
        task_id="run_bronze_ingestion_pipeline",
        python_callable=run_pipeline
    )
    bronze_ingestion_task
    