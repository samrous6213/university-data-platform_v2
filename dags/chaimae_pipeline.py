from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from src.ingestion.api.openalex import run as run_openalex
from src.ingestion.web.uca import crawl_uca


default_args = {
    "owner": "chaimae",
    "depends_on_past": False,
    "start_date": datetime(2025, 6, 9),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    dag_id="chaimae_pipeline",
    default_args=default_args,
    description="OpenAlex + UCA to MinIO",
    schedule="@daily",
    catchup=False,
    tags=["chaimae", "openalex", "uca", "minio"],
)

api_task = PythonOperator(
    task_id="openalex_to_minio",
    python_callable=run_openalex,
    dag=dag,
)

web_task = PythonOperator(
    task_id="uca_to_minio",
    python_callable=crawl_uca,
    dag=dag,
)

api_task >> web_task

print("✅ DAG chaimae_pipeline loaded")