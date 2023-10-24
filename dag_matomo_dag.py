"""
Matomo Data Download DAG

This Airflow DAG is responsible for fetching analytics data from a Matomo instance
and saving it as CSV files to a MinIO bucket. The data fetched ranges from visitor
summaries, action types, referrer sources, and user device types. This DAG is intended
to run daily and fetch the data for the specific day of execution.
"""

from datetime import datetime, timedelta
from airflow.decorators import task, dag
from airflow.hooks.base_hook import BaseHook
import requests
import boto3

DEFAULT_ARGS = {
    'owner': 'nitai',
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

    date_filter = execution_date.strftime("%Y-%m-%d")

    # Matomo API request
    params = {
        'module': 'API',
        'idSite': SITE_ID,
        'period': 'day',
        'date': date_filter,
        'format': 'csv',
        'token_auth': TOKEN_AUTH,
        'method': f'{module}.{method}'
    }
    response = requests.get(MATOMO_URL, params=params)
    if response.status_code == 200:
        return response.text
    else:
        raise Exception(f"Failed to fetch data for {module}.{method}. Status code: {response.status_code}")


def save_to_minio(data, module, method, execution_date):
    # Fetching MinIO connection details from Airflow connections
    minio_conn = BaseHook.get_connection('minio_connection_id')
    MINIO_URL = minio_conn.host
    MINIO_ACCESS_KEY = minio_conn.login
    MINIO_SECRET_KEY = minio_conn.password
    MINIO_BUCKET = minio_conn.schema

    # Saving JSON to MinIO bucket using boto3
    s3_client = boto3.client('s3',
                             endpoint_url=MINIO_URL,
                             aws_access_key_id=MINIO_ACCESS_KEY,
                             aws_secret_access_key=MINIO_SECRET_KEY,
                             region_name='us-east-1')
    filename = f'{module}_{method}_{execution_date.strftime("%Y-%m-%d")}.csv'
    s3_client.put_object(Body=data,
                         Bucket=MINIO_BUCKET,
                         Key=filename,
                         ContentType='text/csv')


@dag(
        default_args=DEFAULT_ARGS,
        schedule_interval='0 5 * * *',
        start_date=datetime(2023, 5, 1),
        catchup=False,
        doc_md=__doc__,
)
def matomo_data_download():
    """
    matomo_data_download DAG

    This DAG orchestrates the fetching of various analytics data points from a Matomo
    instance and saves them as CSV files to a MinIO bucket. It dynamically creates tasks
    for each module-method combination specified in the `endpoints` list and fetches data
    for the specific execution day.

    Returns:
        function: An instance of the matomo_data_download DAG
    """

    # List of endpoints and methods to call
    endpoints = [
        ('VisitsSummary', 'get'),
        ('Actions', 'getPageUrls'),
        ('Actions', 'getPageTitles'),
        ('Actions', 'getOutlinks'),
        ('Referrers', 'getAll'),
        ('UserCountry', 'getCountry'),
        ('UserCountry', 'getRegion'),
        # Add more as necessary
    ]

    for module, method in endpoints:
        @task(task_id=f"extract_from_{method}_{module}")
        def fetch_data(module_: str, method_: str, **context):
            data = get_matomo_data(module_,
                                   method_,
                                   context['data_interval_start']
            )
            save_to_minio(data,
                          module_,
                          method_,
                          context['data_interval_start']
            )

        fetch_data(module, method)

    return matomo_data_download


matomo_data_download()
