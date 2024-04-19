from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
)

from plugins.graphql.hooks.graphql_hook import GraphQLHook


def _get_query():
    """
    Retrieves the query from the specified file and returns it.

    Returns:
    -------
      str: The query string.
    """
    query = Path(__file__).parent.joinpath("./queries/get_meeting_data.gql").open().read()
    return query


DECIDIM_CONN_ID = "api_decidim"
MINIO_CONN_ID = "minio_conn_id"
MINIO_BUCKET = "brasil-participativo-meeting-csv"
LANDING_ZONE_FILE_NAME = "data_meetings/meetings.json"

COMPONENT_TYPE_TO_EXTRACT = "Meeting"
QUERY = _get_query()


def _verify_bucket(hook: S3Hook, bucket_name: str) -> str:
    """
    Verifies if the specified bucket exists in the S3 storage.

    Args:
    ----
      hook (S3Hook): The S3Hook object used to interact with the S3 storage.
      bucket_name (str): The name of the bucket to verify.

    Returns:
    -------
      str: The name of the task to create the bucket if it doesn't exist.
    """
    if not hook.check_for_bucket(bucket_name=bucket_name):
        return "minio_tasks.create_bucket"


def _get_dates(context):
    """
    Get the dates related to the execution context.

    Args:
    ----
      context (dict): The execution context containing the "execution_date" key.

    Returns:
    -------
      tuple: A tuple containing the date, next_date, and date_file.

    Example:
    -------
      >>> context = {"execution_date": datetime.datetime(2022, 1, 1)}
      >>> _get_dates(context)
      ('2022-01-01', '2022-01-02', '20220101')
    """
    date = context["execution_date"].strftime("%Y-%m-%d")
    next_date = (context["execution_date"] + timedelta(days=1)).strftime("%Y-%m-%d")
    date_file = context["execution_date"].strftime("%Y%m%d")
    return date, next_date, date_file


def _task_extract_data(**context):
    """
    Extracts data from a GraphQL API and stores it in a MinIO bucket.

    Args:
    ----
      **context: A dictionary containing the context variables.

    Returns:
    -------
      None
    """
    date, next_date, date_file = _get_dates(context)
    # Fetch data from GraphQL API
    dado = _get_response_graphql(date, next_date)
    # Store data in MinIO bucket
    S3Hook(aws_conn_id=MINIO_CONN_ID).load_string(
        string_data=dado,
        bucket_name=MINIO_BUCKET,
        key=LANDING_ZONE_FILE_NAME,
        replace=True,
    )


def _get_response_graphql(date, next_date):
    """
    Retrieves the response from a GraphQL API for a given date range.

    Args:
    ----
      date (str): The start date of the range.
      next_date (str): The end date of the range.

    Returns:
    -------
      str: The response from the GraphQL API.
    """
    hook = GraphQLHook(DECIDIM_CONN_ID)
    session = hook.get_session()
    response = session.post(
        hook.api_url,
        json={
            "query": QUERY,
            "variables": {"start_date": f"{date}", "end_date": f"{next_date}"},
        },
    )
    # dado = response.json()
    dado = response.text
    return dado


@dag(
    default_args={
        "owner": "Eric/Laura",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    schedule="0 */5 * * *",
    catchup=True,
    start_date=datetime(2023, 11, 10),
    max_active_runs=1,
    description=__doc__,
    tags=["decidim", "reports", "meetings", "bp"],
    dag_id="etl_meetings",
)
def etl_meetings():
    """DAG que extrai dados de reunioes de um GraphQL API e os armazena em um bucket MinIO."""

    @task_group(group_id="minio_tasks")
    def minio_tasks():

        @task.branch()
        def verify_bucket():
            """
            Verifies if the specified bucket exists in the MinIO server.

            Returns:
            -------
              str: The name of the task to create the bucket if it doesn't exist.
            """
            hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
            return _verify_bucket(hook=hook, bucket_name=MINIO_BUCKET)

        create_bucket = S3CreateBucketOperator(
            task_id="create_bucket", bucket_name=MINIO_BUCKET, aws_conn_id=MINIO_CONN_ID
        )
        verify_bucket() >> create_bucket

    @task(provide_context=True, trigger_rule="none_failed")
    def extract_data(**context):
        """
        Fetches data from a GraphQL API and stores it in a MinIO bucket.

        Args:
        ----
          **context: The context dictionary containing the execution date.

        Returns:
        -------
          None
        """
        _task_extract_data(**context)

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    _extract_data = extract_data()
    start >> minio_tasks() >> _extract_data >> end


etl_meetings()
