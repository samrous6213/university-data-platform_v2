"""
Pipeline complet pour [NOM]
Sources: API + Web + PDF
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Configuration spécifique à la personne
PERSON_NAME = "[NOM]"
API_SOURCE = "[SON_API]"  # OpenAlex, Crossref, ORCID
WEB_SOURCE = "[SON_WEB]"   # UM5, UCA, Data.gov
PDF_SOURCE = "[SON_PDF]"   # MIT, Wikipedia, autre

default_args = {
    'owner': PERSON_NAME,
    'start_date': datetime(2025, 6, 9),
    'retries': 3,
}

def extract_api():
    """Extraire depuis [API_SOURCE]"""
    # Ton code API ici
    pass

def extract_web():
    """Extraire depuis [WEB_SOURCE]"""
    # Ton code web scraping ici
    pass

def extract_pdf():
    """Extraire depuis [PDF_SOURCE]"""
    # Ton code PDF ici
    pass

with DAG(
    dag_id=f'{PERSON_NAME}_complete_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
) as dag:
    
    task_api = PythonOperator(
        task_id='extract_api',
        python_callable=extract_api
    )
    
    task_web = PythonOperator(
        task_id='extract_web',
        python_callable=extract_web
    )
    
    task_pdf = PythonOperator(
        task_id='extract_pdf',
        python_callable=extract_pdf
    )
    
    [task_api, task_web, task_pdf]
