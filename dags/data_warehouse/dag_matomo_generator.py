"""
Matomo Data Download DAG.

This Airflow DAG is responsible for fetching analytics data from a Matomo instance
and saving it as CSV files to a MinIO bucket. The data fetched ranges from visitor
summaries, action types, referrer sources, and user device types. This DAG is intended
to run daily and fetch the data for the specific day of execution.
"""

import logging
from datetime import datetime, timedelta
from typing import ClassVar, Dict, List

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

logger = logging.getLogger(__name__)


MINIO_CONN = "minio_conn_id"
MINIO_BUCKET = "matomo-extractions-csv"
POSTGRES_CONN_ID = "conn_postgres"
SCHEMA = "raw"


def _generate_s3_filename(module, method, period, execution_date):
    return f"{module}_{method}_{period}_{execution_date.strftime('%Y-%m-%d')}.csv"


def add_temporal_columns(df: pd.DataFrame, execution_date: datetime) -> pd.DataFrame:
    """
    Adds temporal columns to the DataFrame based on the execution date.

    Args:
    ----
        df (pd.DataFrame): The original DataFrame without temporal columns.
        execution_date (datetime): The execution date to base the temporal columns on.

    Returns:
    -------
        pd.DataFrame: The DataFrame with added temporal columns.
    """
    event_day_id = int(execution_date.strftime("%Y%m%d"))
    available_day = execution_date + timedelta(days=1)
    available_day_id = int(available_day.strftime("%Y%m%d"))
    available_month_id = int(available_day.strftime("%Y%m"))
    available_year_id = int(available_day.strftime("%Y"))
    writing_day_id = int(datetime.now().strftime("%Y%m%d"))

    # Add the temporal columns to the DataFrame
    df["event_day_id"] = event_day_id
    df["available_day_id"] = available_day_id
    df["available_month_id"] = available_month_id
    df["available_year_id"] = available_year_id
    df["writing_day_id"] = writing_day_id

    return df


class MatomoDagGenerator:  # noqa: D101
    endpoints: ClassVar[List] = [
        ("VisitsSummary", "get"),
        ("Actions", "getPageUrls"),
        ("Actions", "getPageTitles"),
        ("Actions", "getOutlinks"),
        ("Referrers", "getAll"),
        ("UserCountry", "getCountry"),
        ("UserCountry", "getRegion"),
    ]
    default_dag_args: ClassVar[Dict] = {
        "owner": "Nitai",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    }

    def get_matomo_data(self, module, method, execution_date, period):
        matomo_conn = BaseHook.get_connection("matomo_conn")
        matomo_url = matomo_conn.host
        token_auth = matomo_conn.password
        site_id = matomo_conn.login

        # Determine the date filter based on the period
        if period == "day":
            date_filter = execution_date.strftime("%Y-%m-%d")
        elif period == "week":
            # Calculate the last day of the previous week
            last_day_of_week = execution_date - timedelta(days=execution_date.weekday() + 1)
            date_filter = last_day_of_week.strftime("%Y-%m-%d")
        elif period == "month":
            # Calculate the last day of the previous month
            first_day_of_month = execution_date.replace(day=1)
            last_day_of_previous_month = first_day_of_month - timedelta(days=1)
            date_filter = last_day_of_previous_month.strftime("%Y-%m-%d")
        else:
            raise ValueError("Invalid period. Choose 'day', 'week', or 'month'.")

        params = {
            "module": "API",
            "idSite": site_id,
            "period": period,
            "date": date_filter,
            "format": "csv",
            "token_auth": token_auth,
            "method": f"{module}.{method}",
        }

        logger.info("Extracting %s data for %s.", period, date_filter)
        response = requests.get(matomo_url, params=params)

        if response.status_code == 200:
            return response.text
        else:
            raise Exception(
                f"Failed to fetch data for {module}.{method}. Status code: {response.status_code}"
            )

    def save_to_minio(self, data, module, method, period, execution_date):

        filename = _generate_s3_filename(module, method, period, execution_date)
        minio = S3Hook(MINIO_CONN)
        minio.load_string(string_data=data, key=filename, bucket_name=MINIO_BUCKET, replace=True)

    def generate_extraction_dag(self, period: str, schedule: str):
        @dag(
            dag_id=f"matomo_extraction_{period}",
            default_args=self.default_dag_args,
            schedule=schedule,
            start_date=datetime(2023, 5, 1),
            catchup=False,
            doc_md=__doc__,
            tags=["matomo", "extraction"],
        )
        def matomo_data_extraction():
            matomo_period = {
                "daily": "day",
                "weekly": "week",
                "monthly": "month",
            }

            start = EmptyOperator(task_id="start")
            end = EmptyOperator(task_id="end")

            task_check_and_create_bucket = S3CreateBucketOperator(
                task_id="check_and_create_bucket", bucket_name=MINIO_BUCKET, aws_conn_id=MINIO_CONN
            )

            start >> task_check_and_create_bucket

            for module, method in self.endpoints:

                @task(task_id=f"extract_{method}_{module}")
                def fetch_data(module_: str, method_: str, **context):
                    data = self.get_matomo_data(
                        module_,
                        method_,
                        context["data_interval_start"],
                        matomo_period[period],
                    )
                    self.save_to_minio(data, module_, method_, period, context["data_interval_start"])

                task_check_and_create_bucket >> fetch_data(module, method) >> end

        return matomo_data_extraction()

    def _ingest_into_postgres(self, module: str, method: str, period: str, execution_date: datetime):
        """
        Ingest data from a CSV file in MinIO into a PostgreSQL database.

        Args:
        ----
            module (str): The name of the module corresponding to the
                          Matomo endpoint.
            method (str): The method name corresponding to the action type.
            execution_date (datetime): The execution date for the DAG run.
        """
        filename = _generate_s3_filename(module, method, period, execution_date)
        csv_path = S3Hook(MINIO_CONN).download_file(bucket_name=MINIO_BUCKET, key=filename)

        # Read the CSV content into a pandas DataFrame
        df = pd.read_csv(csv_path)

        df_with_temporal = add_temporal_columns(df, execution_date)

        # Establish a connection to the PostgreSQL database
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Insert the data into the PostgreSQL table
        df_with_temporal.to_sql(
            name=f"{module}_{method}_{period}",
            con=pg_hook.get_sqlalchemy_engine(),
            if_exists="append",
            index=False,
            schema=SCHEMA,
        )

    def generate_ingestion_dag(self, period: str, schedule: str):
        """Generate an Airflow DAG to ingest data from MinIO into a PostgreSQL database.

        This method sets up the DAG configuration
        and tasks based on predefined endpoints to read CSV files from
        MinIO and append the data to the corresponding tables in PostgreSQL.
        Returns:
        -------
            A callable that when invoked, returns an instance of the
            generated DAG.
        """

        @dag(
            dag_id=f"matomo_ingestion_{period}",
            default_args=self.default_dag_args,
            schedule=schedule,
            start_date=datetime(2023, 5, 1),
            catchup=False,
            doc_md=__doc__,
            tags=["matomo", "ingestion"],
        )
        def matomo_data_ingestion():
            start = EmptyOperator(task_id="start")
            end = EmptyOperator(task_id="end")
            """The main DAG for ingesting data from MinIO into the PostgreSQL database."""

            task_check_and_create_schema = SQLExecuteQueryOperator(
                conn_id=POSTGRES_CONN_ID,
                task_id="check_and_create_schema",
                sql=f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}",
            )

            start >> task_check_and_create_schema

            for module, method in self.endpoints:

                @task(task_id=f"ingest_{method}_{module}")
                def ingest_data(module_: str, method_: str, **context):
                    """Task to ingest data from MinIO into a PostgreSQL database."""
                    self._ingest_into_postgres(module_, method_, period, context["data_interval_start"])

                task_check_and_create_schema >> ingest_data(module, method) >> end

        return matomo_data_ingestion()


# Instantiate the DAGs dynamically
dag_generator = MatomoDagGenerator()
dag_generator.generate_extraction_dag(period="daily", schedule="0 5 * * *")
dag_generator.generate_ingestion_dag(period="daily", schedule="0 7 * * *")
dag_generator.generate_extraction_dag(period="weekly", schedule="0 5 * * 1")
dag_generator.generate_ingestion_dag(period="weekly", schedule="0 7 * * 1")
dag_generator.generate_extraction_dag(period="monthly", schedule="0 5 * 1 *")
dag_generator.generate_ingestion_dag(period="monthly", schedule="0 7 * 1 *")
