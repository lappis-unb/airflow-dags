import io
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
)
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.exc import ProgrammingError

from plugins.graphql.hooks.graphql_hook import GraphQLHook


def _get_query():
    """
    Retrieves the query from the specified file and returns it.

    Returns:
    -------
      str: The query string.
    """
    query = (
        Path(__file__).parent.joinpath("./queries/get_proposals_processes_participative.gql").open().read()
    )
    return query


DECIDIM_CONN_ID = "api_decidim"
MINIO_CONN_ID = "minio_conn_id"
MINIO_BUCKET = "brasil-participativo-daily-csv"
COMPONENT_TYPE_TO_EXTRACT = "Proposals"
TABLE_NAME = "proposals"
SCHEMA = "raw"
PRIMARY_KEY = "proposal_id"
RETRIES = 0
LANDING_ZONE_FILE_NAME = "landing_zone/proposals_{date_file}.json"
PROCESSING_FILE_NAME = "processing/proposals_{date_file}.csv"
PROCESSED_FILE_NAME = "processed/proposals_{date_file}.csv"
POSTGRES_CONN_ID = "conn_postgres"
QUERY = _get_query()


def flatten_structure_with_additional_fields(data):
    """
    Flattens the nested structure of the input data and.

    extracts additional fields for each proposal.

    Args:
    ----
      data (dict): The input data containing nested structure.

    Returns:
    -------
      list: A list of dictionaries, where each dictionary
      represents a flattened proposal with additional fields.

    """
    data = data["data"]["participatoryProcesses"]

    # Function to handle the extraction of text from nested translation dictionaries
    def extract_text(translations):
        if translations and isinstance(translations, list):
            return translations[0].get("text")

    flattened_data = []
    for item in data:
        main_title = extract_text(item.get("title", {}).get("translations", []))
        slug = item.get("slug")
        for component in item.get("components", []):
            component_id = component.get("id", "")
            component_name = extract_text(component.get("name", {}).get("translations", []))
            if "proposals" in component:
                for proposal in component.get("proposals", {}).get("nodes", []):
                    proposal_data = {
                        "main_title": main_title,
                        "slug": slug,
                        "component_id": component_id,
                        "component_name": component_name,
                        "proposal_id": proposal["id"],
                        "proposal_created_at": proposal["createdAt"],
                        "proposal_published_at": proposal.get("publishedAt"),
                        "proposal_updated_at": proposal.get("updatedAt"),
                        "author_name": dict_safe_get(proposal, "author").get("name"),
                        "author_nickname": dict_safe_get(proposal, "author").get("nickname"),
                        "author_organization": dict_safe_get(proposal, "author").get("organizationName"),
                        "proposal_body": extract_text(proposal.get("body", {}).get("translations", [])),
                        "category_name": extract_text(
                            dict_safe_get(dict_safe_get(proposal, "category"), "name").get("translations", [])
                        ),
                        "proposal_title": extract_text(proposal.get("title", {}).get("translations", [])),
                        "authors_count": proposal.get("authorsCount"),
                        "user_allowed_to_comment": proposal.get("userAllowedToComment"),
                        "endorsements_count": proposal.get("endorsementsCount"),
                        "total_comments_count": proposal.get("totalCommentsCount"),
                        "versions_count": proposal.get("versionsCount"),
                        "vote_count": proposal.get("voteCount"),
                        "comments_have_alignment": proposal.get("commentsHaveAlignment"),
                        "comments_have_votes": proposal.get("commentsHaveVotes"),
                        "created_in_meeting": proposal.get("createdInMeeting"),
                        "has_comments": proposal.get("hasComments"),
                        "official": proposal.get("official"),
                        "fingerprint": proposal.get("fingerprint", {}).get("value"),
                        "position": proposal.get("position"),
                        "reference": proposal.get("reference"),
                        "scope": proposal.get("scope"),
                        "state": proposal.get("state"),
                    }
                    flattened_data.append(proposal_data)
    return flattened_data


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


def dict_safe_get(_dict: dict, key: str):
    """
    Retorna o valor associado à chave especificada em um dicionário.

    Se a chave não existir ou o valor for None, retorna um dicionário vazio.

    Args:
    ----
      _dict (dict): O dicionário de onde obter o valor.
      key (str): A chave do valor desejado.

    Returns:
    -------
      O valor associado à chave especificada, ou um
      dicionário vazio se a chave não existir ou o valor for None.
    """
    value = _dict.get(key)
    if not value:
        value = {}
    return value


def _convert_dtype(df: pd.DataFrame) -> pd.DataFrame:
    dtypes = {
        "author_name": "str",
        "author_nickname": "str",
        "author_organization": "str",
        "authors_count": "int",
        "category_name": "str",
        "comments_have_alignment": "bool",
        "comments_have_votes": "bool",
        "component_id": "int",
        "component_name": "str",
        "created_in_meeting": "bool",
        "endorsements_count": "int",
        "fingerprint": "str",
        "has_comments": "bool",
        "main_title": "str",
        "official": "bool",
        "position": "str",
        "proposal_body": "str",
        "proposal_created_at": "datetime64[ns]",
        "proposal_id": "int",
        "proposal_published_at": "datetime64[ns]",
        "proposal_title": "str",
        "proposal_updated_at": "datetime64[ns]",
        "reference": "str",
        "scope": "str",
        "state": "str",
        "slug":"str",
        "total_comments_count": "int",
        "user_allowed_to_comment": "bool",
        "versions_count": "int",
        "vote_count": "int",
    }
    # remove os dtypes que não estão no df
    dtypes = {k: v for k, v in dtypes.items() if k in df.columns}
    return df.astype(dtypes, errors="ignore")


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
        key=LANDING_ZONE_FILE_NAME.format(date_file=date_file),
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


def _task_transform_data(**context):
    """
    Transform the data from the landing zone and save it to MinIO.

    Parameters:
    ----------
    - context: A dictionary containing the execution context.

    Returns:
    -------
    None
    """
    date_file = context["execution_date"].strftime("%Y%m%d")
    minio = S3Hook(aws_conn_id=MINIO_CONN_ID)
    # Read the data from the landing zone
    data = json.loads(
        minio.read_key(
            key=LANDING_ZONE_FILE_NAME.format(date_file=date_file),
            bucket_name=MINIO_BUCKET,
        )
    )
    df = _get_df_transform_data(data)
    _save_minio_processing(date_file, minio, df)


def _save_minio_processing(date_file, minio, df):
    """
    Save the DataFrame as a CSV file in MinIO.

    Args:
    ----
      date_file (str): The date of the file.
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
        key=PROCESSING_FILE_NAME.format(date_file=date_file),
        replace=True,
    )


def _get_df_transform_data(data):
    """
    Transforms the input data into a pandas DataFrame.

    Args:
    ----
      data (dict): The input data to be transformed.

    Returns:
    -------
      pandas.DataFrame: The transformed DataFrame.
    """
    data = flatten_structure_with_additional_fields(data)
    df = pd.DataFrame(data)
    df = _convert_dtype(df)
    if len(df) > 0:
        df.columns = df.columns.str.lower()
    return df


def _delete_landing_zone_file(context):
    """
    Deletes the landing zone file for a given execution date.

    Args:
    ----
      context (dict): The context object containing the execution date.

    Returns:
    -------
      None
    """
    date_file = context["execution_date"].strftime("%Y%m%d")
    minio = S3Hook(aws_conn_id=MINIO_CONN_ID)
    minio.delete_objects(
        bucket=MINIO_BUCKET,
        keys=LANDING_ZONE_FILE_NAME.format(date_file=date_file),
    )


def _check_empty_file(**context):
    """
    Checks if the file is empty.

    Args:
    ----
      context (dict): The context dictionary containing the execution date.

    Returns:
    -------
      str: The task ID to be executed next based on whether the file is empty or not.
    """
    minio = S3Hook(aws_conn_id=MINIO_CONN_ID)
    date_file = context["execution_date"].strftime("%Y%m%d")
    dado = minio.read_key(
        key=PROCESSING_FILE_NAME.format(date_file=date_file),
        bucket_name=MINIO_BUCKET,
    )
    if len(dado.strip()) == 0:
        logging.warning("No data found for %s.", date_file)
        return "load.empty_file"
    return "load.check_and_create_table"


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
    has_table = engine.has_table(table_name=TABLE_NAME, schema=SCHEMA)

    if not has_table:
        engine.execute(
            f"""
          CREATE TABLE {SCHEMA}.{TABLE_NAME} (
            main_title text NULL,
            slug text NULL,
            component_id int8 NULL,
            component_name text NULL,
            proposal_id int8 NOT NULL,
            proposal_created_at timestamp NULL,
            proposal_published_at timestamp NULL,
            proposal_updated_at timestamp NULL,
            author_name text NULL,
            author_nickname text NULL,
            author_organization text NULL,
            proposal_body text NULL,
            category_name text NULL,
            proposal_title text NULL,
            authors_count int8 NULL,
            user_allowed_to_comment bool NULL,
            endorsements_count int8 NULL,
            total_comments_count int8 NULL,
            versions_count int8 NULL,
            vote_count int8 NULL,
            comments_have_alignment bool NULL,
            comments_have_votes bool NULL,
            created_in_meeting bool NULL,
            has_comments bool NULL,
            official bool NULL,
            fingerprint text NULL,
            position int8 NULL,
            reference text NULL,
            scope text NULL,
            state text NULL,
            event_day_id int8 NULL,
            available_day_id int8 NULL,
            available_month_id int8 NULL,
            available_year_id int8 NULL,
            writing_day_id int8 NULL
          );
          """
        )
        engine.execute(f"ALTER TABLE {SCHEMA}.{TABLE_NAME} ADD PRIMARY KEY ({PRIMARY_KEY});")


def _task_get_ids_from_table(engine):
    """
    Retrieves the proposal IDs from a specified table in the database.

    Args:
    ----
      engine (sqlalchemy.engine.Engine): The SQLAlchemy engine object used to connect to the database.

    Returns:
    -------
      list: A list of proposal IDs retrieved from the table.

    Raises:
    ------
      None

    """
    with engine.connect() as connection:
        try:
            result = connection.execute(f"SELECT proposal_id FROM {SCHEMA}.{TABLE_NAME};")
        except ProgrammingError as error:
            logging.warning("Table does not exist. Error: %s", error)
            return []
        proposal_ids = [row[0] for row in result]
    return proposal_ids


def _save_data_postgres(proposal_ids, **context):
    df = _get_df_save_data_postgres(context)
    df = _transform_data_save_data_postgres(proposal_ids, context, df)
    ## Configure the postgres hook and insert the data
    _save_table_save_data_postgres(df)


def _save_table_save_data_postgres(df):
    """
    Saves the given DataFrame to a PostgreSQL table.

    Args:
    ----
      df (pandas.DataFrame): The DataFrame to be saved.

    Returns:
    -------
      None
    """
    engine = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_sqlalchemy_engine()

    df.to_sql(TABLE_NAME, con=engine, schema=SCHEMA, if_exists="append", index=False)


def _transform_data_save_data_postgres(proposal_ids, context, df):
    """
    Transform df removing the proposal_ids and adding temporal columns.

    Args:
    ----
      proposal_ids (list): A list of proposal_ids to filter out from the DataFrame.
      context (dict): A dictionary containing the execution context of the function.
      df (pandas.DataFrame): The input DataFrame to be transformed.

    Returns:
    -------
      pandas.DataFrame: The transformed DataFrame after removing rows and adding temporal columns.
    """
    df = df[~df["proposal_id"].isin(proposal_ids)]
    df = add_temporal_columns(df, context["execution_date"])
    return df


def _get_df_save_data_postgres(context):
    """
    Reads a CSV file from an S3 bucket and returns a pandas DataFrame.

    Args:
    ----
      context (dict): The context object containing execution information.

    Returns:
    -------
      pandas.DataFrame: The DataFrame containing the data from the CSV file.
    """
    date_file = context["execution_date"].strftime("%Y%m%d")
    minio = S3Hook(aws_conn_id=MINIO_CONN_ID)
    data = minio.read_key(
        key=PROCESSING_FILE_NAME.format(date_file=date_file),
        bucket_name=MINIO_BUCKET,
    )
    csv_file = io.StringIO(data)
    df = pd.read_csv(csv_file)
    return df


def _task_move_file_s3(context):
    """
    Move a file from the source bucket to the destination bucket in S3.

    Args:
    ----
      context (dict): The context dictionary containing the execution date.

    Returns:
    -------
      None
    """
    date_file = context["execution_date"].strftime("%Y%m%d")
    source_filename = PROCESSING_FILE_NAME.format(date_file=date_file)
    dest_filename = PROCESSED_FILE_NAME.format(date_file=date_file)
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)

    s3_hook.copy_object(
        source_bucket_key=source_filename,
        dest_bucket_key=dest_filename,
        source_bucket_name=MINIO_BUCKET,
        dest_bucket_name=MINIO_BUCKET,
    )
    s3_hook.delete_objects(bucket=MINIO_BUCKET, keys=source_filename)


@dag(
    default_args={
        "owner": "Amoêdo/Nitai",
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
    },
    schedule="0 23 * * *",
    catchup=True,
    start_date=datetime(2023, 11, 10),
    max_active_runs=10,
    description=__doc__,
    tags=["decidim", "minio"],
    dag_id="etl_proposals",
)
def etl_proposals():
    """DAG que extrai dados de propostas de um GraphQL API e os armazena em um bucket MinIO."""

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

    @task_group
    def transform():
        @task(provide_context=True, retry_delay=timedelta(seconds=5))
        def transform_data(**context):
            """
            Transforms the data by flattening the structure and saving it as a CSV file in MinIO.

            Args:
            ----
              **context: The context dictionary containing the execution date.

            Returns:
            -------
              None
            """
            _task_transform_data(**context)

        @task(
            provide_context=True,
        )
        def delete_landing_zone_file(**context):
            """
            Deletes a file from the landing zone in MinIO.

            Args:
            ----
              context (dict): The context dictionary containing the execution date.

            Returns:
            -------
              None
            """
            _delete_landing_zone_file(context)

        transform_data() >> delete_landing_zone_file()

    @task_group(
        group_id="load",
    )
    def load():
        empty_file = EmptyOperator(task_id="empty_file")

        @task.branch(provide_context=True)
        def check_empty_file(**context):
            """
            Checks if the file is empty.

            This function takes in the context as input and checks if the
            file specified in the context is empty or not.
            It returns the result of the check.

            Parameters:
            ----------
            - context: A dictionary containing the context variables.

            Returns:
            -------
            - bool: True if the file is empty, False otherwise.
            """
            return _check_empty_file(**context)

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

        @task(provide_context=True, retry_delay=timedelta(minutes=3))
        def get_ids_from_table(**context):
            """
            Gets the proposal IDs from the 'proposals' table in the database.

            Args:
            ----
              context (dict): The context dictionary containing execution information.

            Returns:
            -------
              list: A list of proposal IDs from the table.
            """
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            engine = hook.get_sqlalchemy_engine()
            return _task_get_ids_from_table(engine)

        @task(provide_context=True, retry_delay=timedelta(minutes=3))
        def save_data_postgres(proposal_ids: list, **context):
            """
            Save data to PostgreSQL.

            Args:
            ----
              proposal_ids (list): A list of proposal IDs to save.
              context (dict): Additional context information.

            Returns:
            -------
              None
            """
            _save_data_postgres(proposal_ids, **context)

        @task(
            provide_context=True,
            trigger_rule="one_success",
        )
        def move_file_s3(**context):
            """
            Moves a file from the source bucket to the destination bucket in MinIO.

            Args:
            ----
              context (dict): The context dictionary containing the execution date.

            Returns:
            -------
              None
            """
            _task_move_file_s3(context)

        _create_table = check_and_create_table()
        _move_file_s3 = move_file_s3()
        _get_ids_from_table = get_ids_from_table()
        check_empty_file() >> [empty_file, _create_table]
        (_create_table >> _get_ids_from_table >> save_data_postgres(_get_ids_from_table) >> _move_file_s3)
        empty_file >> _move_file_s3

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    _extract_data = extract_data()
    start >> minio_tasks() >> _extract_data >> transform() >> load() >> end


etl_proposals()
