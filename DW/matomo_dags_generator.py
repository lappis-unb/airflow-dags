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
from airflow.hooks.postgres_hook import PostgresHook
import requests
import boto3
import pandas as pd
from io import StringIO

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


def add_temporal_columns(
        df: pd.DataFrame,
        execution_date: datetime) -> pd.DataFrame:
    """
    Adds temporal columns to the DataFrame based on the execution date.

    Args:
        df (pd.DataFrame): The original DataFrame without temporal columns.
        execution_date (datetime): The execution date to base the temporal columns on.

    Returns:
        pd.DataFrame: The DataFrame with added temporal columns.
    """
    event_day_id = int(execution_date.strftime('%Y%m%d'))
    available_day = execution_date + timedelta(days=1)
    available_day_id = int(available_day.strftime('%Y%m%d'))
    available_month_id = int(available_day.strftime('%Y%m'))
    available_year_id = int(available_day.strftime('%Y'))
    writing_day_id = int(datetime.now().strftime('%Y%m%d'))

    # Add the temporal columns to the DataFrame
    df['event_day_id'] = event_day_id
    df['available_day_id'] = available_day_id
    df['available_month_id'] = available_month_id
    df['available_year_id'] = available_year_id
    df['writing_day_id'] = writing_day_id

    return df


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
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 3,
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


    def generate_extraction_dag(self, period: str, schedule: str):
        @dag(
                dag_id=f'matomo_extraction_{period}',
                default_args=self.default_dag_args,
                schedule=schedule,
                start_date=datetime(2023, 5, 1),
                catchup=False,
                doc_md=__doc__,
        )
        def matomo_data_extraction():
            matomo_period = {
                'daily': 'day',
                'weekly': 'week',
                'monthly': 'month',
            }
            for module, method in self.endpoints:
                @task(task_id=f"extract_{method}_{module}")
                def fetch_data(module_: str, method_: str, **context):
                    data = self.get_matomo_data(
                        module_,
                        method_,
                        context['data_interval_start'],
                        matomo_period[period])
                    self.save_to_minio(
                        data,
                        module_,
                        method_,
                        period,
                        context['data_interval_start'])

                fetch_data(module, method)

        return matomo_data_extraction()


    def _ingest_into_postgres(self,
                              module: str,
                              method: str,
                              execution_date: datetime):
        """
        Ingest data from a CSV file in MinIO into a PostgreSQL database.

        Args:
            module (str): The name of the module corresponding to the
                          Matomo endpoint.
            method (str): The method name corresponding to the action type.
            execution_date (datetime): The execution date for the DAG run.
        """
        s3_client = _create_s3_client()
        minio_conn = BaseHook.get_connection('minio_connection_id')
        filename = _generate_s3_filename(module, method, execution_date)

        obj = s3_client.get_object(Bucket=minio_conn.schema, Key=filename)
        csv_content = obj['Body'].read().decode('utf-8')

        # Read the CSV content into a pandas DataFrame
        df = pd.read_csv(StringIO(csv_content))

        df_with_temporal = add_temporal_columns(df, execution_date)

        # Establish a connection to the PostgreSQL database
        pg_hook = PostgresHook(postgres_conn_id='dw_postgres')

        # Insert the data into the PostgreSQL table
        df_with_temporal.to_sql(
            name=f'{module}_{method}',
            con=pg_hook.get_sqlalchemy_engine(),
            if_exists='append',
            index=False,
        )


    def generate_ingestion_dag(self, period: str, schedule: str):
        """
        Generate an Airflow DAG to ingest data from MinIO into a
        PostgreSQL database. This method sets up the DAG configuration
        and tasks based on predefined endpoints to read CSV files from
        MinIO and append the data to the corresponding tables in PostgreSQL.
        Returns:
            A callable that when invoked, returns an instance of the
            generated DAG.
        """
        @dag(
            dag_id=f'matomo_ingestion_{period}',
            default_args=self.default_dag_args,
            schedule=schedule,
            start_date=datetime(2023, 5, 1),
            catchup=False,
            doc_md=__doc__,
        )
        def matomo_data_ingestion():
            """The main DAG for ingesting data from MinIO into the
            PostgreSQL database.
            """
            for module, method in self.endpoints:

                @task(task_id=f"ingest_{method}_{module}")
                def ingest_data(module_: str, method_: str, **context):
                    """Task to ingest data from MinIO into a PostgreSQL
                    database.
                    """
                    self._ingest_into_postgres(
                        module_, method_, context['data_interval_start']
                    )

                ingest_data(module, method)

        return matomo_data_ingestion()


# Instantiate the DAGs dynamically
dag_generator = MatomoDagGenerator()
dag_generator.generate_extraction_dag(period='daily', schedule='0 5 * * *')
dag_generator.generate_ingestion_dag(period='daily', schedule='0 7 * * *')
dag_generator.generate_extraction_dag(period='weekly', schedule='0 5 * * 1')
dag_generator.generate_ingestion_dag(period='weekly', schedule='0 7 * * 1')
