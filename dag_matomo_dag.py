from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task, dag
from airflow.hooks.base_hook import BaseHook
import requests
import json
import boto3

DEFAULT_ARGS = {
    'owner': 'you',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_matomo_data(module, method, execution_date):
    # Fetching Matomo connection details from Airflow connections
    matomo_conn = BaseHook.get_connection('matomo_connection_id')
    MATOMO_URL = matomo_conn.host
    TOKEN_AUTH = matomo_conn.password
    SITE_ID = matomo_conn.login

    # Matomo API request
    params = {
        'module': 'API',
        'idSite': SITE_ID,
        'period': 'day',
        'date': execution_date.isoformat(),
        'format': 'CSV',
        'token_auth': TOKEN_AUTH,
        'method': f'{module}.{method}'
    }
    response = requests.get(MATOMO_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data for {module}.{method}. Status code: {response.status_code}")

def save_to_minio(data, module, method, execution_date):
    # Fetching MinIO connection details from Airflow connections
    minio_conn = BaseHook.get_connection('minio_connection_id')
    MINIO_URL = minio_conn.host
    MINIO_ACCESS_KEY = minio_conn.login
    MINIO_SECRET_KEY = minio_conn.password

    # Saving JSON to MinIO bucket using boto3
    s3_client = boto3.client('s3',
                             endpoint_url=MINIO_URL,
                             aws_access_key_id=MINIO_ACCESS_KEY,
                             aws_secret_access_key=MINIO_SECRET_KEY,
                             region_name='us-east-1')
    filename = f"{module}_{method}_{execution_date}.json"
    s3_client.put_object(Body=json.dumps(data),
                         Bucket='bucket-test',
                         Key=filename)

@dag(
        default_args=DEFAULT_ARGS,
        schedule_interval='@daily',
        start_date=datetime(2023, 5, 1),
        catchup=False,
)
def matomo_data_download():

    # List of endpoints and methods to call
    endpoints = [
        ('VisitsSummary', 'get'),
        ('Actions', 'getPageUrls'),
        ('Actions', 'getPageTitles'),
        ('Actions', 'getDownload'),
        ('Actions', 'getOutlinks'),
        ('Referrers', 'getAll'),
        ('Referrers', 'getWebsites'),
        ('Referrers', 'getSearchEngines'),
        ('UserCountry', 'getCountry'),
        ('DeviceDetection', 'getType'),
        # Add more as necessary
    ]

    for module, method in endpoints:
        @task(task_id=f"extract_from_{method}_{module}")
        def fetch_data(module_: str, method_: str, **context):
            # import ipdb; ipdb.set_trace()
            data = get_matomo_data(module_, method_, context['execution_date'])
            save_to_minio(data, module_, method_, context['execution_date'])

        fetch_data(module, method)  # This call will generate a unique task based on the current module and method

    return matomo_data_download

dag = matomo_data_download()
