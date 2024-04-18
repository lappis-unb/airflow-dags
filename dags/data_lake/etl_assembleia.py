from datetime import timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.utils.task_group import TaskGroup
from pendulum import datetime

from plugins.graphql.hooks.graphql_hook import GraphQLHook

# Some important constants
API_DECIDIM = "api_decidim"
MINIO_CONN = "minio_conn_id"
MINIO_BUCKET = "brasil-participativo-confjuv"
LANDING_ZONE_FILE_NAME = "landing_zone/raw_assembleia.json"


# Auxiliary functions
def _get_query() -> str:
    """
    Retrieves the query from the specified file and returns it.

    Returns:
    -------
      str: The query string.
    """
    return Path(__file__).parent.joinpath("./queries/confjuv_query.gql").open().read()


def _run_graphql_query() -> str:
    """
    Perform a GET request to the GraphQL API to retrieve the data.

    Args:
    ----
    None
    Returns:
    -------
      str: The data from the GraphQL API.
    """
    hook = GraphQLHook(API_DECIDIM)
    session = hook.get_session()
    response = session.post(url=hook.api_url, json={"query": _get_query()})
    return response.text


@dag(
    schedule_interval="@once",
    start_date=datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    default_args={
        "owner": "Rodolfo",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
    },
    max_active_runs=10,
    description="Esta DAG recebe os dados da Conferência da Juventude",
    tags=["etl_assembly_conferencia_juventude"],
    dag_id="etl_assembly_conferencia_juventude",
)
def etl_assembly_conferencia_juventude():
    start = EmptyOperator(task_id="start")

    with TaskGroup("extraction", tooltip="Extract data from GraphQL to MinIO bucket") as extraction:

        new_bucket = S3CreateBucketOperator(
            task_id="create_bucket", bucket_name=MINIO_BUCKET, aws_conn_id=MINIO_CONN
        )

        @task(task_id="extract_data")
        def extract_data(**kwargs):

            query_data = _run_graphql_query()

            S3Hook(aws_conn_id=MINIO_CONN).load_string(
                string_data=query_data,
                bucket_name=MINIO_BUCKET,
                key=LANDING_ZONE_FILE_NAME,
                replace=True,
            )

        new_bucket >> extract_data()

    end = EmptyOperator(task_id="end")

    # Execution order
    start >> extraction >> end


etl_assembly_conferencia_juventude()
