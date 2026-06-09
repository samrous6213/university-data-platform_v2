#template for everyone to copy and modify for their own source. Just change the SOURCE_NAME and implement the extraction logic in the extract_and_save_to_minio function. The Spark job will be the same for everyone, just make sure to pass the correct source_path from MinIO.
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

SOURCE_NAME = "template_source"  # CHANGE THIS
MINIO_BUCKET = "raw_web_json"
HUDI_TABLE = "faculty_profiles"

default_args = {
    'owner': 'team',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 1),
    'email_on_failure': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id=f'university_ingest_{SOURCE_NAME}',
    default_args=default_args,
    description=f'Ingest {SOURCE_NAME} -> MinIO -> Spark -> Hudi',
    schedule_interval='@daily',
    catchup=False,
    tags=['ingestion', SOURCE_NAME],
) as dag:

    def extract_and_save_to_minio(**context):
        import requests
        from minio import Minio
        import json
        import hashlib
        
        # YOUR EXTRACTION LOGIC HERE
        print(f"Extracting from {SOURCE_NAME}")
        
        # For testing, create dummy data
        test_data = {
            "source": SOURCE_NAME,
            "timestamp": context['ds'],
            "data": [{"id": 1, "name": "test"}]
        }
        
        # Connect to MinIO (will implement later)
        client = Minio(
            "minio:9000", 
            access_key="minioadmin", 
            secret_key="minioadmin", 
            secure=False
        )
        
        run_id = context['ds']
        object_path = f"{SOURCE_NAME}/year={run_id[:4]}/data_{run_id}.json"
        
        # This will fail until MinIO is running - that's OK for now
        try:
            client.put_object(
                MINIO_BUCKET, 
                object_path, 
                data=json.dumps(test_data).encode('utf-8'), 
                length=len(json.dumps(test_data))
            )
            context['task_instance'].xcom_push(key='source_path', value=object_path)
        except Exception as e:
            print(f"MinIO not ready yet: {e}")
            # Still push a path for testing
            context['task_instance'].xcom_push(key='source_path', value=object_path)
        
        return f"Saved to {object_path}"

    extract_task = PythonOperator(
        task_id='extract_save_minio',
        python_callable=extract_and_save_to_minio,
        provide_context=True,
    )

    transform_task = SparkSubmitOperator(
        task_id='transform_to_hudi',
        application='/opt/airflow/spark_jobs/transform_hudi.py',
        name=f'{SOURCE_NAME}_to_hudi',
        conn_id='spark_default',
        verbose=True,
        application_args=[
            '--source_path', "{{ task_instance.xcom_pull(task_ids='extract_save_minio', key='source_path') }}",
            '--hudi_table', HUDI_TABLE,
            '--source_name', SOURCE_NAME
        ]
    )

    extract_task >> transform_task
