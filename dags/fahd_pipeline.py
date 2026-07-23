import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


logger = logging.getLogger(__name__)

default_args = {
    "owner": "fahd",
    "depends_on_past": False,
    "start_date": datetime(2025, 6, 9),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


def run_openalex():
    # Delay imports so the DAG file can be parsed even if optional runtime deps
    # (e.g., minio) are not installed in the Airflow image.
    from src.ingestion.api.Fahd_openalex import run as _run_openalex

    return _run_openalex()


#def crawl_uca():
    from src.ingestion.web.Fahd_ import crawl_uca as _crawl_uca

    return _crawl_uca()


with DAG(
    dag_id="fahd_pipeline",
    default_args=default_args,
    description="OpenAlex + UCA to MinIO",
    schedule="@daily",
    catchup=False,
    tags=["Fahd", "openalex", "uca", "minio"],
) as dag:

    api_task = PythonOperator(
        task_id="openalex_to_minio",
        python_callable=run_openalex,
    )

    #web_task = PythonOperator(task_id="uca_to_minio",python_callable=crawl_uca,)

    #api_task >> web_task
