import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

default_args = {
    "owner": "hiba",
    "depends_on_past": False,
    "start_date": datetime(2025, 6, 9),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


def run_openalex():
    from src.ingestion.api.hiba_openalex import run as _run_openalex
    return _run_openalex()


def crawl_univh2c():
    from src.ingestion.web.hiba_uh2c import crawl_uh2c as _crawl_univh2c
    return _crawl_univh2c()


with DAG(
    dag_id="hiba_pipeline",
    default_args=default_args,
    description="OpenAlex + UnivH2C to MinIO",
    schedule="@daily",
    catchup=False,
    tags=["hiba", "openalex", "univh2c", "minio"],
) as dag:

    api_task = PythonOperator(
        task_id="openalex_to_minio",
        python_callable=run_openalex,
    )

    web_task = PythonOperator(
        task_id="univh2c_to_minio",
        python_callable=crawl_univh2c,
    )

    api_task >> web_task