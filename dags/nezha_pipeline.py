"""
DAG unique : orchestre l'ensemble du pipeline University Data Platform.
Flux :
    Ingestion (USMS, MIT OCW, Crossref) [parallèle]
        -> clean_data
        -> write_hudi (lit les données brutes, applique les 4 transformations
           en interne via transform_faculty/courses/publications/news, puis
           écrit les tables Hudi et les synchronise avec Hive)
        -> export_to_postgres
        -> index_elasticsearch
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "nezha",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

HUDI_PACKAGES = (
    "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

SPARK_SUBMIT_BASE = (
    "docker exec spark-master /opt/spark/bin/spark-submit "
    "--conf spark.jars.ivy=/tmp/ivy "
    f"--packages {HUDI_PACKAGES} "
)


def python_with_pythonpath(script_path: str) -> str:
    return (
        "docker exec spark-master bash -c "
        f"\"cd /workspace && PYTHONPATH=/workspace python3 {script_path}\""
    )


with DAG(
    dag_id="nezha_pipeline",
    default_args=default_args,
    description="Pipeline complet : ingestion -> Hudi -> Postgres -> Elasticsearch",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["university", "etl", "hudi", "elasticsearch"],
) as dag:

    ingest_usms = BashOperator(
        task_id="ingest_usms",
        bash_command=python_with_pythonpath("src/ingestion/web/usms_vf/usms.py"),
    )
    ingest_mit_ocw = BashOperator(
        task_id="ingest_mit_ocw",
        bash_command=python_with_pythonpath("src/ingestion/docs/mit_ocw_reader.py"),
    )
    ingest_crossref = BashOperator(
        task_id="ingest_crossref",
        bash_command=python_with_pythonpath("src/ingestion/api/crossref.py"),
    )

    clean_data = BashOperator(
        task_id="clean_data",
        bash_command=SPARK_SUBMIT_BASE + "/workspace/src/transformations/spark/clean_data.py",
    )

    write_hudi = BashOperator(
        task_id="write_hudi",
        bash_command=SPARK_SUBMIT_BASE + "/workspace/src/transformations/spark/write_hudi.py",
    )

    export_to_postgres = BashOperator(
        task_id="export_to_postgres",
        bash_command=SPARK_SUBMIT_BASE + "/workspace/src/transformations/spark/export_to_postgres.py",
    )

    index_elasticsearch = BashOperator(
        task_id="index_elasticsearch",
        bash_command=python_with_pythonpath("src/search/elasticsearch/index.py"),
    )

    [ingest_usms, ingest_mit_ocw, ingest_crossref] >> clean_data
    clean_data >> write_hudi >> export_to_postgres >> index_elasticsearch