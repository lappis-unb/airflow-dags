"""
Matomo Data Download DAG

This Airflow DAG is responsible for fetching analytics data from a Matomo instance
and saving it as CSV files to a MinIO bucket. The data fetched ranges from visitor
summaries, action types, referrer sources, and user device types. This DAG is intended
to run daily and fetch the data for the specific day of execution.
"""
import logging

from datetime import datetime, timedelta
from airflow.decorators import task, dag
from airflow.hooks.base_hook import BaseHook
import requests
import boto3

logger = logging.getLogger(__name__)


def _create_s3_client():
    minio_conn = BaseHook.get_connection('minio_connection_id')
    MINIO_URL = minio_conn.host
    MINIO_ACCESS_KEY = minio_conn.login
    MINIO_SECRET_KEY = minio_conn.password

    return boto3.client('s3',
                        endpoint_url=MINIO_URL,
                        aws_access_key_id=MINIO_ACCESS_KEY,
                        aws_secret_access_key=MINIO_SECRET_KEY,
                        region_name='us-east-1')

def _generate_s3_filename(module, method, execution_date):
    return f'{module}_{method}_{execution_date.strftime("%Y-%m-%d")}.csv'


class MatomoDagGenerator:

    endpoints = [
        ('VisitsSummary', 'get'),
        ('Actions', 'getPageUrls'),
        ('Actions', 'getPageTitles'),
        ('Actions', 'getOutlinks'),
        ('Referrers', 'getAll'),
        ('UserCountry', 'getCountry'),
        ('UserCountry', 'getRegion'),
    ]
    default_dag_args = {
        'owner': 'nitai',
        'depends_on_past': False,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }


    def get_matomo_data(self, module, method, execution_date):
        matomo_conn = BaseHook.get_connection('matomo_connection_id')
        MATOMO_URL = matomo_conn.host
        TOKEN_AUTH = matomo_conn.password
        SITE_ID = matomo_conn.login
        date_filter = execution_date.strftime("%Y-%m-%d")

        params = {
            'module': 'API',
            'idSite': SITE_ID,
            'period': 'day',
            'date': date_filter,
            'format': 'csv',
            'token_auth': TOKEN_AUTH,
            'method': f'{module}.{method}'
        }

        logger.info('Extracting data for %s', date_filter)
        response = requests.get(MATOMO_URL, params=params)

        if response.status_code == 200:
            return response.text
        else:
            raise Exception(f"Failed to fetch data for {module}.{method}. Status code: {response.status_code}")

    def save_to_minio(self, data, module, method, execution_date):
        minio_conn = BaseHook.get_connection('minio_connection_id')
        MINIO_BUCKET = minio_conn.schema
        s3_client = _create_s3_client()
        filename = _generate_s3_filename(module, method, execution_date)
        s3_client.put_object(Body=data,
                             Bucket=MINIO_BUCKET,
                             Key=filename,
                             ContentType='text/csv')

    def generate_dag(self):
        @dag(
                default_args=self.default_dag_args,
                schedule_interval='0 5 * * *',
                start_date=datetime(2023, 5, 1),
                catchup=False,
                doc_md=__doc__,
        )
        def matomo_data_extractor():
            for module, method in self.endpoints:
                @task(task_id=f"extract_from_{method}_{module}")
                def fetch_data(module_: str, method_: str, **context):
                    data = self.get_matomo_data(module_,
                                            method_,
                                            context['data_interval_start']
                    )
                    self.save_to_minio(data,
                                module_,
                                method_,
                                context['data_interval_start']
                    )
                fetch_data(module, method)

        return matomo_data_extractor()


dag_generator = MatomoDagGenerator()
dag_generator.generate_dag()
