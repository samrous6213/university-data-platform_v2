from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json
import boto3

def run():
    import requests
    import json
    import boto3
    import traceback

    try:
        url = "https://api.crossref.org/works?query=AI&rows=5"
        data = requests.get(url).json()

        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin"
        )

        s3.put_object(
            Bucket="data-lake",
            Key="raw/crossref/sara_test.json",
            Body=json.dumps(data).encode("utf-8")
        )

        print("SUCCESS PIPELINE")

    except Exception as e:
        print("ERROR OCCURED:")
        print(traceback.format_exc())
        raise

with DAG(
    dag_id="sara_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    PythonOperator(
        task_id="run_pipeline",
        python_callable=run
    )