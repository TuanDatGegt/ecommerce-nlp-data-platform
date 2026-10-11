## airflow/dags/silver_cleaning_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from pipeline.silver import run_pipeline as run_silver_pipeline

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="silver_cleaning_dag",
    default_args=default_args,
    description="Làm sạch, validate và loại bỏ trùng lặp dữ liệu từ Bronze sang Silver (DuckDB)",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["pipeline", "silver", "cleaning"],
) as dag:

    task_silver_cleaning = PythonOperator(
        task_id="run_silver_cleaning",
        python_callable=run_silver_pipeline,
    )

    task_silver_cleaning
