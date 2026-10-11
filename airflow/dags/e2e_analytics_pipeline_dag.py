## airflow/dags/e2e_analytics_pipeline_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from pipeline.bronze import run_pipeline as run_bronze_pipeline
from pipeline.silver import run_pipeline as run_silver_pipeline
from pipeline.gold import run_pipeline as run_gold_pipeline

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="e2e_analytics_pipeline_dag",
    default_args=default_args,
    description="Chuỗi xử lý dữ liệu tự động hóa End-to-End: Bronze -&gt; Silver -&gt; Gold",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["e2e", "master_pipeline", "analytics"],
) as dag:

    task_bronze = PythonOperator(
        task_id="step_1_bronze_ingestion",
        python_callable=run_bronze_pipeline,
    )

    task_silver = PythonOperator(
        task_id="step_2_silver_cleaning",
        python_callable=run_silver_pipeline,
    )

    task_gold = PythonOperator(
        task_id="step_3_gold_aggregations",
        python_callable=run_gold_pipeline,
    )

    # Đặt thứ tự phụ thuộc (Pipeline Dependency)
    task_bronze >> task_silver >> task_gold
