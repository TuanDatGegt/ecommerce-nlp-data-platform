## airflow/dags/gold_feature_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
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
    dag_id="gold_feature_dag",
    default_args=default_args,
    description="Trích xuất thuộc tính và tổng hợp chỉ số báo cáo tầng Gold (DuckDB)",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["pipeline", "gold", "analytics"],
) as dag:

    task_gold_feature = PythonOperator(
        task_id="run_gold_feature",
        python_callable=run_gold_pipeline,
    )

    task_gold_feature
