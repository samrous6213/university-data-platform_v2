import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

default_args = {
    "owner": "ayoub",
    "depends_on_past": False,
    "start_date": datetime(2025, 6, 9),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


def run_orcid():
    from src.ingestion.api.ayoub_ORCID import run as _run_orcid
    return _run_orcid()


def crawl_usmba():
    from src.ingestion.web.usmba import crawl_usmba as _crawl_usmba
    return _crawl_usmba()


def run_datagov():
    # On importe la fonction run du nouveau fichier datagov
    from src.ingestion.docs.datagov import run as _run_datagov
    
    # On définit la nouvelle URL du document XLS à télécharger (Lien officiel qui fonctionne 100%)
    url = "https://data.gov.ma/data/fr/dataset/d4589781-4f02-4fbf-9317-2088b315fa97/resource/df6bb4cc-b694-4520-9637-69700e52817f/download/etab-ensprimaire-public-men-2013-2014-2.xls"
    
    return _run_datagov(document_url=url, source_name="datagov")


with DAG(
    dag_id="ayoub_pipeline",
    default_args=default_args,
    description="ORCID + USMBA + DATAGOV to MinIO",
    schedule="@daily",
    catchup=False,
    tags=["ayoub", "orcid", "usmba", "datagov", "minio"],
) as dag:

    api_task = PythonOperator(
        task_id="orcid_to_minio",
        python_callable=run_orcid,
    )

    web_task = PythonOperator(
        task_id="usmba_to_minio",
        python_callable=crawl_usmba,
    )

    doc_task = PythonOperator(
        task_id="datagov_to_minio",
        python_callable=run_datagov,
    )

    # Ordre d'exécution des tâches (Pipeline séquentiel)
    api_task >> web_task >> doc_task