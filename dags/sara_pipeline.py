# dags/sara_pipeline.py
"""
DAG Airflow pour l'ingestion et la transformation des données - SARA
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'sara',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    dag_id='sara_university_pipeline',
    default_args=default_args,
    description='Pipeline ingestion UM5 - Toubkal - Crossref - Spark - Elasticsearch',
    schedule_interval='@daily',
    catchup=False,
    tags=['sara', 'ingestion', 'transformation'],
)

# ===== TÂCHES =====

# 1. Scraper UM5
scrape_um5 = BashOperator(
    task_id='scrape_um5',
    bash_command='cd /opt/airflow && PYTHONPATH=/opt/airflow python -m src.ingestion.web.um5',
    dag=dag,
    execution_timeout=timedelta(minutes=60),
    retries=0,
)

# 2. Scraper Toubkal
scrape_toubkal = BashOperator(
    task_id='scrape_toubkal',
    bash_command='cd /opt/airflow && PYTHONPATH=/opt/airflow python -m src.ingestion.docs.toubkal',
    dag=dag,
    execution_timeout=timedelta(minutes=60),
    retries=0,
)

# 3. Scraper Crossref
scrape_crossref = BashOperator(
    task_id='scrape_crossref',
    bash_command='cd /opt/airflow && PYTHONPATH=/opt/airflow python -m src.ingestion.api.crossref',
    dag=dag,
    execution_timeout=timedelta(minutes=60),
    retries=0,
)

# 4. Transformation Spark
transform_spark = BashOperator(
    task_id='transform_spark',
    bash_command='cd /opt/airflow && PYTHONPATH=/opt/airflow python -m src.processing.spark_transform',
    dag=dag,
)

# 5. Indexation Elasticsearch
index_elasticsearch = BashOperator(
    task_id='index_elasticsearch',
    bash_command='cd /opt/airflow && PYTHONPATH=/opt/airflow python -m src.processing.index_to_elasticsearch',
    dag=dag,
)

# ===== DÉPENDANCES =====
[scrape_um5, scrape_toubkal, scrape_crossref] >> transform_spark >> index_elasticsearch