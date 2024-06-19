import io
import json
import logging
from datetime import timedelta
from pathlib import Path
from typing import Any

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from pandas import DataFrame, read_csv
from pendulum import datetime
from sqlalchemy.exc import ProgrammingError

from plugins.graphql.hooks.graphql_hook import GraphQLHook

# Some important constants
API_DECIDIM = "api_decidim"
MINIO_CONN = "minio_conn_id"
PROPOSALS_TABLE_NAME = "confjuv_proposals"
MEETINGS_TABLE_NAME = "confjuv_meetings"
POSTS_TABLE_NAME = "confjuv_posts"
MINIO_BUCKET = "brasil-participativo-confjuv"
LANDING_ZONE_FILE_NAME = "landing_zone/raw_assembleia.json"
PROPOSALS_PROCESSING_ZONE_FILE = "processing_zone/proposals.csv"
MEETING_PROCESSING_ZONE_FILE = "processing_zone/meeting.csv"
POSTS_PROCESSING_ZONE_FILE = "processing_zone/posts.csv"
POSTGRES_CONN_ID = "conn_postgres"
SCHEMA = "raw_confjuv"


# Auxiliary functions

def _get_proposals_csv_data():
    """
    Reads a CSV file from an S3 bucket and returns a pandas DataFrame.

    Args:
    ----

    Returns:
    -------
      pandas.DataFrame: The DataFrame containing the data from the CSV file.
    """
    minio = S3Hook(aws_conn_id=MINIO_CONN)
    data = minio.read_key(
        key=PROPOSALS_PROCESSING_ZONE_FILE,
        bucket_name=MINIO_BUCKET,
    )
    csv_file = io.StringIO(data)
    return read_csv(csv_file, index_col=1)


def _get_meetings_csv_data():
    """
    Reads a CSV file from an S3 bucket and returns a pandas DataFrame.

    Args:
    ----
    Returns:
    -------
      pandas.DataFrame: The DataFrame containing the data from the CSV file.
    """
    minio = S3Hook(aws_conn_id=MINIO_CONN)
    data = minio.read_key(
        key=MEETING_PROCESSING_ZONE_FILE,
        bucket_name=MINIO_BUCKET,
    )

    csv_file = io.StringIO(data)
    return read_csv(csv_file)


def _get_posts_csv_data():
    """
    Reads a CSV file from an S3 bucket and returns a pandas DataFrame.

    Args:
    ----

    Returns:
    -------
      pandas.DataFrame: The DataFrame containing the data from the CSV file.
    """
    minio = S3Hook(aws_conn_id=MINIO_CONN)
    data = minio.read_key(
        key=POSTS_PROCESSING_ZONE_FILE,
        bucket_name=MINIO_BUCKET,
    )
    csv_file = io.StringIO(data)
    return read_csv(csv_file)


def _check_and_create_table(engine):
    """
    Check if the table exists in the database and create it if it doesn't exist.

    Args:
    ----
      engine (sqlalchemy.engine.Engine): The SQLAlchemy engine object.

    Returns:
    -------
      None
    """
    # Checks if there exists the proposals table
    has_proposals_table = engine.has_table(table_name=PROPOSALS_TABLE_NAME, schema=SCHEMA)
    if not has_proposals_table:
        engine.execute(
            f"""
          CREATE TABLE {SCHEMA}.{PROPOSALS_TABLE_NAME} (
            id INT NOT NULL PRIMARY KEY,
            createdAt timestamp NULL,
            updatedAt timestamp NULL,
            type text NULL,
            totalCommentsCount int NULL,
            title text NULL,
            reference text NULL,
            author_name text NULL,
            author_avatarUrl text NULL,
            author_badge text NULL,
            author_deleted bool NULL,
            author_organizationName text NULL,
            author_nickname text NULL,
            body text NULL,
            comments_status text NULL,
            comments_body text NULL,
            comments_id int NULL,
            comments_author_name text NULL,
            comments_author_avatarUrl text NULL,
            comments_author_badge text NULL,
            comments_author_deleted bool NULL,
            comments_author_organizationName text NULL,
            comments_author_nickname text NULL
          );
          """
        )

    # Checks if there exists the meetings table
    has_meeting_table = engine.has_table(table_name=MEETINGS_TABLE_NAME, schema=SCHEMA)
    if not has_meeting_table:
        engine.execute(
            f"""
          CREATE TABLE {SCHEMA}.{MEETINGS_TABLE_NAME} (
            id INT NOT NULL PRIMARY KEY,
            startTime timestamp NULL,
            endTime timestamp NULL,
            closed bool NULL,
            title text NULL,
            address text NULL
          );
          """
        )

    # Checks if there exists the posts table
    has_posts_table = engine.has_table(table_name=POSTS_TABLE_NAME, schema=SCHEMA)
    if not has_posts_table:
        engine.execute(
            f"""
          CREATE TABLE {SCHEMA}.{POSTS_TABLE_NAME} (
            id INT NOT NULL PRIMARY KEY,
            body text NULL,
            author text NULL,
            title text NULL,
            createdAt timestamp NULL
          );
          """
        )


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


def _check_and_create_schema(engine, schema):
    """
    Check if the schema exists in the database and create it if it doesn't exist.

    Args:
    ----
      engine (sqlalchemy.engine.Engine): The SQLAlchemy engine object.

    Returns:
    -------
      None
    """
    with engine.connect() as connection:
        try:
            connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        except ProgrammingError as e:
            logging.error("Error creating schema: %s", e)


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


def _flat_data(raw: list) -> dict:
    """
    Flattens the nested structure of the input data.

    Args:
    ----
        raw (list): The input data containing nested structure.

    Returns:
    -------
        dict: A dictionary representing a flattened structure.
    """
    ret = {}

    for element in raw:
        for key in element:
            if isinstance(element[key], dict):
                for sub_key in element[key]:
                    new_key = f"{key}_{sub_key}"
                    if new_key not in ret:
                        ret[new_key] = []
                    if isinstance(element[key][sub_key], (list, tuple)):
                        ret[new_key].extend(element[key][sub_key])
                    else:
                        ret[new_key].append(element[key][sub_key])

            elif isinstance(element[key], list):
                nested_flattened = _flat_data(element[key])
                for nested_key in nested_flattened:
                    new_key = f"{key}_{nested_key}"
                    if new_key not in ret:
                        ret[new_key] = []
                    ret[new_key].extend(nested_flattened[nested_key])

            else:
                if key not in ret:
                    ret[key] = []
                ret[key].append(element[key])

    return ret


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
        "transform", tooltip="Retrieve the data from MinIO bucket to be converted into CSV file"
    ) as transform:

        def _save_minio_processing(minio, df, filename: str):
            """
            Save the DataFrame as a CSV file in MinIO.

            Args:
            ----
            minio (MinioClient): The MinIO client object.
            df (pandas.DataFrame): The DataFrame to be saved.

            Returns:
            -------
            None
            """
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            minio.load_string(
                string_data=csv_buffer.getvalue(),
                bucket_name=MINIO_BUCKET,
                key=filename,
                replace=True,
            )

        @task(task_id="save_to_csv")
        def save_to_csv(**context):
            """
            Retrieves the data from MinIO and saves into a CSV file.

            Returns:
            -------
            None
            """
            minio = S3Hook(aws_conn_id=MINIO_CONN)
            data = json.loads(minio.read_key(key=LANDING_ZONE_FILE_NAME, bucket_name=MINIO_BUCKET))
            data = data["data"]["assembly"]

            # Extracts the title
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
            posts = _flat_data(posts)
            proposals = _flat_data(proposals)
            meetings = _flat_data(meetings)

            proposals = DataFrame.from_dict(data=proposals)
            meetings = DataFrame.from_dict(data=meetings)
            posts = DataFrame.from_dict(data=posts)

            proposals.columns = [x.lower() for x in proposals.columns]
            meetings.columns = [x.lower() for x in meetings.columns]
            posts.columns = [x.lower() for x in posts.columns]

            _save_minio_processing(minio, proposals, PROPOSALS_PROCESSING_ZONE_FILE)
            _save_minio_processing(minio, meetings, MEETING_PROCESSING_ZONE_FILE)
            _save_minio_processing(minio, posts, POSTS_PROCESSING_ZONE_FILE)

        @task(task_id="delete_landing_zone_file")
        def delete_landing_zone_file(**context):
            """
            Deletes the landing zone file for a given execution date.

            Args:
            ----
            context (dict): The context object containing the execution date.

            Returns:
            -------
            None
            """
            minio = S3Hook(aws_conn_id=MINIO_CONN)
            minio.delete_objects(
                bucket=MINIO_BUCKET,
                keys=LANDING_ZONE_FILE_NAME,
            )

        save_to_csv() >> delete_landing_zone_file()

    with TaskGroup("load", tooltip="Loads the data from CSV file and stores into a database") as load:

        empty_file = EmptyOperator(task_id="empty_file")

        @task.branch(task_id="check_empty_file", provide_context=True)
        def check_empty_file(**context):
            """
            Checks if the CSV files's empty.

            Args:
            ----
            context (dict): The context dictionary containing the execution date.

            Returns:
            -------
            str: The task ID to be executed next based on whether the files's empty or not.
            """
            minio = S3Hook(aws_conn_id=MINIO_CONN)
            proposals = minio.read_key(
                key=PROPOSALS_PROCESSING_ZONE_FILE,
                bucket_name=MINIO_BUCKET,
            )
            meetings = minio.read_key(
                key=MEETING_PROCESSING_ZONE_FILE,
                bucket_name=MINIO_BUCKET,
            )
            posts = minio.read_key(
                key=POSTS_PROCESSING_ZONE_FILE,
                bucket_name=MINIO_BUCKET,
            )
            if len(proposals.strip()) == 0 and len(posts.strip()) and len(meetings.strip()):
                return "load.empty_file"
            return "load.check_and_create_table"

        @task(retry_delay=timedelta(minutes=3))
        def check_and_create_table():
            """
            Check if the table exists in the database and create it if it doesn't exist.

            This task connects to a PostgreSQL database using
            the PostgresHook and checks if a table with the specified name
            and schema exists. If the table doesn't exist, it
            creates a new table with the specified columns and primary key.

            Args:
            ----
              None

            Returns:
            -------
              None
            """
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            engine = hook.get_sqlalchemy_engine()

            _check_and_create_schema(engine, SCHEMA)
            _check_and_create_table(engine)

        @task(task_id="save_to_database", trigger_rule="none_failed")
        def save_to_database():
            csv_proposals = _get_proposals_csv_data()
            csv_meetings = _get_meetings_csv_data()
            csv_posts = _get_posts_csv_data()

            engine = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_sqlalchemy_engine()
            csv_proposals.to_sql(
                PROPOSALS_TABLE_NAME, con=engine, schema=SCHEMA, if_exists="replace", index=False
            )
            csv_meetings.to_sql(
                MEETINGS_TABLE_NAME, con=engine, schema=SCHEMA, if_exists="replace", index=False
            )
            csv_posts.to_sql(POSTS_TABLE_NAME, con=engine, schema=SCHEMA, if_exists="replace", index=False)

        check_empty_file() >> [check_and_create_table(), empty_file] >> save_to_database()

    end = EmptyOperator(task_id="end")

    # Execution order
    start >> extraction >> transform >> load >> end


etl_assembly_conferencia_juventude()
