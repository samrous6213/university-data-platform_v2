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
    from src.ingestion.web.chaimae_uca_faculty import run as _crawl_uca
    return _crawl_uca()


def download_toubkal_pdfs():
    from src.ingestion.docs.chaimae_imist import extract_imist_documents
    extract_imist_documents()


def run_etl_pipeline():
    import docker
    client = docker.from_env()
    container = client.containers.get("spark-master")
    cmd = (
        "/opt/spark/bin/spark-submit "
        "--master local[1] --driver-memory 4g "
        "--conf spark.executorEnv.PYTHONPATH=/opt/spark/work-dir "
        "/opt/spark/work-dir/src/transformations/run_all_etl.py"
    )
    exit_code, output = container.exec_run(
        cmd=cmd,
        environment={
            "PYTHONPATH": "/opt/spark/work-dir",
            "ES_HOST": "university-elasticsearch",
        },
        workdir="/opt/spark/work-dir",
        stderr=True,
    )
    decoded = output.decode() if isinstance(output, bytes) else str(output)
    if exit_code != 0:
        raise RuntimeError(f"Spark ETL failed (exit={exit_code}):\n{decoded}")
    logger.info("Spark ETL completed successfully")
    return decoded


with DAG(
    dag_id="chaimae_pipeline",
    default_args=default_args,
    description="Ingestion → Spark ETL → Elasticsearch indexing",
    schedule="@daily",
    catchup=False,
    tags=["chaimae", "openalex", "uca", "imist", "etl", "elasticsearch"],
) as dag:

    api_task = PythonOperator(
        task_id="openalex_to_minio",
        python_callable=run_openalex,
    )

    web_task = PythonOperator(
        task_id="uca_to_minio",
        python_callable=crawl_uca,
    )

    pdf_task = PythonOperator(
        task_id="imist_pdfs_to_minio",
        python_callable=download_toubkal_pdfs,
    )

    etl_task = PythonOperator(
        task_id="spark_etl_to_elasticsearch",
        python_callable=run_etl_pipeline,
        execution_timeout=timedelta(minutes=30),
    )

    # Ingestion (parallel) → ETL + indexing
    [api_task, web_task, pdf_task] >> etl_task