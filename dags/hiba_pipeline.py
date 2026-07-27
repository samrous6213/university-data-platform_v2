import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

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

SPARK_ETL_CONF = {
    "spark.driverEnv.PYTHONPATH": "/opt/spark/work-dir",
    "spark.executorEnv.PYTHONPATH": "/opt/spark/work-dir",
    "spark.hadoop.fs.s3a.endpoint": "http://university-minio:9000",
    "spark.hadoop.fs.s3a.access.key": "minioadmin",
    "spark.hadoop.fs.s3a.secret.key": "minioadmin",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.extensions": "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
}

with DAG(
    dag_id="hiba_pipeline",
    default_args=default_args,
    description="Ingestion + ETL + Indexing pipeline",
    schedule=None,
    catchup=False,
    tags=["hiba", "ingestion", "etl", "minio"],
) as dag:

    # ── Ingestion tasks ──

    ingest_openalex = BashOperator(
        task_id="ingest_openalex",
        bash_command="python /opt/spark/work-dir/src/ingestion/api/hiba_openalex.py",
    )

    ingest_uh2c = BashOperator(
        task_id="ingest_uh2c",
        bash_command="python /opt/spark/work-dir/src/ingestion/web/hiba_uh2c.py",
    )

    ingest_hcp = BashOperator(
        task_id="ingest_hcp",
        bash_command="python /opt/spark/work-dir/src/ingestion/docs/hiba_hcp.py",
    )

    # ── ETL tasks (SparkSubmitOperator → spark://spark-master:7077) ──

    faculty_profiles_etl = SparkSubmitOperator(
        task_id="faculty_profiles_etl",
        application="/opt/spark/work-dir/src/transformations/spark/faculty_profiles_etl.py",
        conn_id="spark_default",
        deploy_mode="client",
        verbose=True,
        application_args=[],
        conf=SPARK_ETL_CONF,
    )

    research_publications_etl = SparkSubmitOperator(
        task_id="research_publications_etl",
        application="/opt/spark/work-dir/src/transformations/spark/research_publications_etl.py",
        conn_id="spark_default",
        deploy_mode="client",
        verbose=True,
        application_args=[],
        conf=SPARK_ETL_CONF,
    )

    university_news_etl = SparkSubmitOperator(
        task_id="university_news_etl",
        application="/opt/spark/work-dir/src/transformations/spark/university_news_etl.py",
        conn_id="spark_default",
        deploy_mode="client",
        verbose=True,
        application_args=[],
        conf=SPARK_ETL_CONF,
    )

    documents_registry_etl = SparkSubmitOperator(
        task_id="documents_registry_etl",
        application="/opt/spark/work-dir/src/transformations/spark/documents_registry_etl.py",
        conn_id="spark_default",
        deploy_mode="client",
        verbose=True,
        application_args=[],
        conf=SPARK_ETL_CONF,
    )

    # ── Index tasks ──

    index_faculty_profiles = BashOperator(
        task_id="index_faculty_profiles",
        bash_command="/opt/spark/bin/spark-submit /opt/spark/work-dir/src/search/index_faculty_profiles.py",
    )

    index_research_publications = BashOperator(
        task_id="index_research_publications",
        bash_command="/opt/spark/bin/spark-submit /opt/spark/work-dir/src/search/index_research_publications.py",
    )

    index_university_news = BashOperator(
        task_id="index_university_news",
        bash_command="/opt/spark/bin/spark-submit /opt/spark/work-dir/src/search/index_university_news.py",
    )

    index_documents_registry = BashOperator(
        task_id="index_documents_registry",
        bash_command="/opt/spark/bin/spark-submit /opt/spark/work-dir/src/search/index_documents_registry.py",
    )

    # ── Dependency chain ──

    (
        [ingest_openalex, ingest_uh2c, ingest_hcp]
        >> faculty_profiles_etl
        >> research_publications_etl
        >> university_news_etl
        >> documents_registry_etl
        >> index_faculty_profiles
        >> index_research_publications
        >> index_university_news
        >> index_documents_registry
    )
