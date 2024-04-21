import json
import logging
from datetime import timedelta
from pathlib import Path
from typing import Any

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


def _sanitize_data(type: str, raw: list, keys: list) -> list:
    proposals: list = []
    translation_list = ["title", "body"]

    if type == "proposals":
        for i in raw:
            elem = i["node"]
            data = {}
            for key in keys:
                if key in elem:
                    if key in translation_list:
                        tmp = _extract_text(elem[key])
                        data[key] = tmp if tmp is not None else elem[key]
                        continue
                    data[key] = elem[key]
            proposals.append(data)
    else:
        for elem in raw:
            data = {}
            for key in keys:
                if key in elem:
                    if key in translation_list:
                        tmp = _extract_text(elem[key])
                        data[key] = tmp if tmp is not None else elem[key]
                    else:
                        data[key] = elem[key]
            proposals.append(data)
    return proposals


def _log(title: str, obj: Any) -> None:
    """
    Logs the obj in a fancy look.

    Returns:
    -------
    None
    """
    info = f"==============START {title}'s LOGGING==============="
    logging.info(info)
    logging.info(obj)
    logging.info("==============END LOGGING===============")


def _extract_text(translations: dict):
    if translations["translations"] and isinstance(translations["translations"], list):
        return translations["translations"][0].get("text")


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
        "retries": 0,
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

    with TaskGroup(
        "transformation", tooltip="Retrieve the data from MinIO bucket to be transformed"
    ) as tranformation:

        @task(task_id="transform")
        def transform():
            """
            Retrieves the data from MinIO and saves into a csv file.

            Returns:
            -------
            None
            """
            minio = S3Hook(aws_conn_id=MINIO_CONN)
            data = json.loads(minio.read_key(key=LANDING_ZONE_FILE_NAME, bucket_name=MINIO_BUCKET))
            data = data["data"]["assembly"]

            # Extracts the title
            title = _extract_text(data["title"])
            _log("TITLE", title)
            proposals = _sanitize_data(
                "proposals",
                data["components"][0]["proposals"]["edges"],
                [
                    "updatedAt",
                    "createdAt",
                    "type",
                    "totalCommentsCount",
                    "title",
                    "reference",
                    "author",
                    "body",
                    "id",
                    "comments",
                ],
            )
            meetings = _sanitize_data(
                "meetings",
                data["components"][1]["meetings"]["nodes"],
                ["id", "startTime", "endTime", "closed", "title", "address"],
            )
            posts = _sanitize_data(
                "posts",
                data["components"][4]["posts"]["nodes"],
                ["body", "author", "endTime", "createdAt", "title"],
            )
            _log("PROPOSTAS", proposals)
            _log("MEETINGS", meetings)
            _log("POSTS", posts)

        transform()

    end = EmptyOperator(task_id="end")

    # Execution order
    start >> extraction >> tranformation >> end


etl_assembly_conferencia_juventude()
