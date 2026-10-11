# airflow/dags/bronze_ingestion_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from pipeline.bronze import run_pipeline as run_bronze_pipeline

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bronze_ingestion_dag",
    default_args=default_args,
    description="Nạp dữ liệu thô từ Kaggle API vào Data Lake tầng Bronze (Parquet)",
    schedule_interval=None,  # Chạy theo nhu cầu hoặc Trigger từ Master DAG
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["pipeline", "bronze", "ingestion"],
) as dag:

    task_bronze_ingestion = PythonOperator(
        task_id="run_bronze_ingestion",
        python_callable=run_bronze_pipeline,
    )

    task_bronze_ingestion
