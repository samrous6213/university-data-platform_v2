"""
DAG pour ayoub
Sources:
- API: À définir
- Web: À définir  
- PDF: À définir
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# TODO: Importer les fonctions depuis src/
# from src.ingestion.api.xxx import extract_xxx
# from src.ingestion.web.xxx import scrape_xxx
# from src.ingestion.docs.xxx import parse_xxx

default_args = {
    'owner': 'ayoub',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

# Fonctions temporaires pour la structure
def extract_api_temp():
    print("TODO: Implémenter extraction API pour ayoub")
    return True

def extract_web_temp():
    print("TODO: Implémenter extraction Web pour ayoub")
    return True

def extract_pdf_temp():
    print("TODO: Implémenter extraction PDF pour ayoub")
    return True

dag = DAG(
    dag_id='ayoub_pipeline',
    default_args=default_args,
    description='Pipeline complet pour ayoub',
    schedule_interval='@daily',
    catchup=False,
    tags=['ayoub', 'api', 'web', 'pdf'],
)

api_task = PythonOperator(
    task_id='extract_api',
    python_callable=extract_api_temp,
    dag=dag,
)

web_task = PythonOperator(
    task_id='extract_web',
    python_callable=extract_web_temp,
    dag=dag,
)

pdf_task = PythonOperator(
    task_id='extract_pdf',
    python_callable=extract_pdf_temp,
    dag=dag,
)

[api_task, web_task, pdf_task]

print(f"✅ DAG pour ayoub chargé")
