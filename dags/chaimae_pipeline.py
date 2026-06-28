import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

default_args = {
    "owner": "chaimae",
    "depends_on_past": False,
    "start_date": datetime(2025, 6, 9),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


def run_openalex():
    from src.ingestion.api.chaimae_openalex import run as _run_openalex
    return _run_openalex()


def crawl_uca():
    from src.ingestion.web.chaimae_uca import crawl_uca as _crawl_uca
    return _crawl_uca()


# CORRECTION : fonction manquante dans le DAG original
def download_toubkal_pdfs():
    from ingestion.docs.chaimae_imist import run as _run_toubkal
    return _run_toubkal()


with DAG(
    dag_id="chaimae_pipeline",
    default_args=default_args,
    description="OpenAlex + UCA + imist PDFs to MinIO",
    schedule="@daily",
    catchup=False,
    tags=["chaimae", "openalex", "uca", "imist", "minio"],
) as dag:

    api_task = PythonOperator(
        task_id="openalex_to_minio",
        python_callable=run_openalex,
    )

    web_task = PythonOperator(
        task_id="uca_to_minio",
        python_callable=crawl_uca,
    )

    # CORRECTION : python_callable pointe maintenant sur la fonction définie ci-dessus
    pdf_task = PythonOperator(
        task_id="imist_pdfs_to_minio",
        python_callable=download_toubkal_pdfs,
    )

    # Ordre d'exécution : OpenAlex → UCA → Toubkal PDFs
    api_task >> web_task >> pdf_task