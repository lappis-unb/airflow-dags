import io
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteObjectsOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
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
    query = Path(__file__).parent.joinpath("./queries/get_meeting_data.gql").open().read()
    return query


DECIDIM_CONN_ID = "api_decidim"
MINIO_CONN_ID = "minio_conn_id"
MINIO_BUCKET = "brasil-participativo"
COMPONENT_TYPE_TO_EXTRACT = "Meetings"
TABLE_NAME = "meetings"
SCHEMA = "raw"
PRIMARY_KEY = "meeting_id"
RETRIES = 0
LANDING_ZONE_FILE_NAME = "dag_meetings/landing_zone/meetings.json"
PROCESSING_FILE_NAME = "dag_meetings/processing/meetings.csv"
PROCESSED_FILE_NAME = "dag_meetings/processed/meetings.csv"
POSTGRES_CONN_ID = "conn_postgres"
QUERY = _get_query()


def flatten_structure_with_additional_fields(data):
    """
    Flattens the nested structure of the input data and.

    extracts additional fields for each meeting.

    Args:
    ----
      data (dict): The input data containing nested structure.

    Returns:
    -------
      list: A list of dictionaries, where each dictionary
      represents a flattened meeting with additional fields.

    """
    data = data["data"]["participatoryProcesses"]
    assert data is not None and isinstance(data, object), "Object is None or unexpected"

    # Function to handle the extraction of text from nested translation dictionaries
    def extract_text(translations):
        if not translations or not isinstance(translations, list):
            return None

        return translations[0].get("text")

    flattened_data = []
    for item in data:
        main_title = extract_text(item.get("title", {}).get("translations", []))
        assert main_title, "Main title is empty"

        for component in item.get("components", []):
            if "meetings" in component:
                for meeting in component.get("meetings", {}).get("nodes", []):
                    meeting_data = {
                        "meeting_id": meeting.get("id"),
                        "main_title": main_title,
                        "title_meeting": meeting.get("title", {}).get("translation"),
                        "category_id": dict_safe_get(meeting, "category").get("id"),
                        "category_name": dict_safe_get(meeting, "category")
                        .get("name", {})
                        .get("translation"),
                        "typeOfMeeting": meeting.get("typeOfMeeting"),
                        "total_comments_count": meeting.get("totalCommentsCount"),
                        "start_time": meeting.get("startTime"),
                        "end_time": meeting.get("endTime"),
                        "attendee_count": meeting.get("attendeeCount"),
                        "address": meeting.get("address"),
                        "coordinates_latitude": dict_safe_get(meeting, "coordinates").get("latitude"),
                        "coordinates_longitude": dict_safe_get(meeting, "coordinates").get("longitude"),
                    }
                    flattened_data.append(meeting_data)

    return flattened_data


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
    dado_graphql = _get_response_graphql(date, next_date)
    # Store data in MinIO bucket
    S3Hook(aws_conn_id=MINIO_CONN_ID).load_string(
        string_data=dado_graphql,
        bucket_name=MINIO_BUCKET,
        key=LANDING_ZONE_FILE_NAME,
        replace=True,
    )


def _get_response_graphql(initial_date, next_date):
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
    hook_decidim = GraphQLHook(DECIDIM_CONN_ID)
    session = hook_decidim.get_session()
    response = session.post(
        hook_decidim.api_url,
        json={
            "query": QUERY,
            "variables": {"start_date": f"{initial_date}", "end_date": f"{next_date}"},
        },
    )
    # dado = response.json()
    dado_json = response.text
    return dado_json


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
    minio = S3Hook(aws_conn_id=MINIO_CONN_ID)
    assert isinstance(minio, S3Hook), "minio is not an instance of S3Hook"

    # Read the data from the landing zone
    data = json.loads(
        minio.read_key(
            key=LANDING_ZONE_FILE_NAME,
            bucket_name=MINIO_BUCKET,
        )
    )

    assert isinstance(data, dict), "Data loaded for landing zone is not a JSON"

    df = _get_df_transform_data(data)
    _save_minio_processing(minio, df)


def _get_df_transform_data(dados):
    """
    Transforms the input data into a pandas DataFrame.

    Args:
    ----
      data (dict): The input data to be transformed.

    Returns:
    -------
      pandas.DataFrame: The transformed DataFrame.
    """
    dados = flatten_structure_with_additional_fields(dados)
    data_frame = pd.DataFrame(dados)
    data_frame = _convert_dtype(data_frame)

    if len(data_frame) > 0:
        data_frame.columns = data_frame.columns.str.lower()
    return data_frame


def _convert_dtype(df: pd.DataFrame) -> pd.DataFrame:
    dtypes = {
        "meeting_id": "int",
        "main_title": "str",
        "title_meeting": "str",
        "category_id": "int",
        "category_name": "str",
        "typeOfMeeting": "str",
        "total_comments_count": "int",
        "start_time": "datetime64[ns]",
        "end_time": "datetime64[ns]",
        "attendee_count": "int",
        "address": "str",
        "coordinates_latitude": "int",
        "coordinates_longitude": "int",
    }
    # remove os dtypes que não estão no df
    dtypes = {k: v for k, v in dtypes.items() if k in df.columns}
    return df.astype(dtypes, errors="ignore")


def _save_minio_processing(minio_var, data_frame):
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
    data_frame.to_csv(csv_buffer, index=False)
    minio_var.load_string(
        string_data=csv_buffer.getvalue(),
        bucket_name=MINIO_BUCKET,
        key=PROCESSING_FILE_NAME,
        replace=True,
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
    dado = minio.read_key(
        key=PROCESSING_FILE_NAME,
        bucket_name=MINIO_BUCKET,
    )
    if len(dado.strip()) == 0:
        logging.warning("No data found for.")
        return "load.empty_file"
    return "load.create_schema_and_table"


def _task_get_ids_from_table(engine):
    """
    Retrieves the meeting IDs from a specified table in the database.

    Args:
    ----
      engine (sqlalchemy.engine.Engine): The SQLAlchemy engine object used to connect
      to the meeting_ide database.

    Returns:
    -------
      list: A list of meeting IDs retrieved from the table.

    Raises:
    ------
      None

    """
    with engine.connect() as connection:
        try:
            result = connection.execute(f"SELECT meeting_id FROM {SCHEMA}.{TABLE_NAME};")
        except ProgrammingError as error:
            logging.warning("Table does not exist. Error: %s", error)
            return []
        meeting_ids = [row[0] for row in result]
    return meeting_ids


def _save_data_postgres(meeting_ids, **context):
    df = _get_df_save_data_postgres(context)
    df = _transform_data_save_data_postgres(meeting_ids, context, df)
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
    minio = S3Hook(aws_conn_id=MINIO_CONN_ID)
    data = minio.read_key(
        key=PROCESSING_FILE_NAME,
        bucket_name=MINIO_BUCKET,
    )

    csv_file = io.StringIO(data)
    df = pd.read_csv(csv_file)
    return df


def _transform_data_save_data_postgres(meeting_ids, context, df):
    """
    Transform df removing the meeting_ids and adding temporal columns.

    Args:
    ----
      meeting_ids (list): A list of meeting_ids to filter out from the DataFrame.
      context (dict): A dictionary containing the execution context of the function.
      df (pandas.DataFrame): The input DataFrame to be transformed.

    Returns:
    -------
      pandas.DataFrame: The transformed DataFrame after removing rows and adding temporal columns.
    """
    df = df[~df["meeting_id"].isin(meeting_ids)]

    assert not df["meeting_id"].isin(meeting_ids).any(), "Exist meeting_ids presents for lists `meeting_ids`."

    df = add_temporal_columns(df, context["execution_date"])

    return df


def add_temporal_columns(data_frame: pd.DataFrame, processing_data: datetime) -> pd.DataFrame:
    """
    Adds temporal columns to the DataFrame based on the execution date.

    Args:
    ----
        data_frame (pd.DataFrame): The original DataFrame without temporal columns.
        processing_data (datetime): The execution date to base the temporal columns on.

    Returns:
    -------
        pd.DataFrame: The DataFrame with added temporal columns.
    """
    assert isinstance(data_frame, pd.DataFrame), "data_frame must be a pandas.DataFrame"

    assert isinstance(processing_data, datetime), "processing_data must be a datetime object"

    event_day_id = int(processing_data.strftime("%Y%m%d"))
    available_day = processing_data + timedelta(days=1)
    available_day_id = int(available_day.strftime("%Y%m%d"))
    available_month_id = int(available_day.strftime("%Y%m"))
    available_year_id = int(available_day.strftime("%Y"))
    writing_day_id = int(datetime.now().strftime("%Y%m%d"))

    # Add the temporal columns to the DataFrame
    data_frame["event_day_id"] = event_day_id
    data_frame["available_day_id"] = available_day_id
    data_frame["available_month_id"] = available_month_id
    data_frame["available_year_id"] = available_year_id
    data_frame["writing_day_id"] = writing_day_id

    return data_frame


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
    source_filename = PROCESSING_FILE_NAME
    dest_filename = PROCESSED_FILE_NAME
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
        "owner": "Eric/Laura",
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
    },
    schedule="0 */50 * * *",
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

        create_bucket = S3CreateBucketOperator(
            task_id="create_bucket", bucket_name=MINIO_BUCKET, aws_conn_id=MINIO_CONN_ID
        )
        create_bucket.execute(context=None)

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

        delete_landing_zone_file = S3DeleteObjectsOperator(
            task_id="delete_landing_zone_file",
            bucket=MINIO_BUCKET,
            keys=LANDING_ZONE_FILE_NAME,
            aws_conn_id=MINIO_CONN_ID,
        )

        transform_data() >> delete_landing_zone_file

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

        create_schema_and_table = SQLExecuteQueryOperator(
            task_id="create_schema_and_table",
            conn_id=POSTGRES_CONN_ID,
            autocommit=True,
            database="data_warehouse",
            sql="""
            CREATE SCHEMA IF NOT EXISTS {{params.schema}};

            CREATE TABLE IF NOT EXISTS {{params.schema}}.{{params.table_name}} (
            meeting_id int8 NULL,
            main_title text NULL,
            title_meeting text NULL,
            category_id int8 NULL,
            category_name text NULL,
            typeOfmeeting text NULL,
            total_comments_count text NULL,
            start_time timestamp NULL,
            event_day_id int8 NULL,
            available_day_id int8 NULL,
            available_month_id int8 NULL,
            available_year_id int8 NULL,
            writing_day_id int8 NULL,
            end_time timestamp NULL,
            attendee_count int8 NULL,
            address text NULL,
            coordinates_latitude int8 NULL,
            coordinates_longitude int8 NULL
          );
          """,
            params={"schema": SCHEMA, "table_name": TABLE_NAME, "primary_key": PRIMARY_KEY},
        )

        @task(provide_context=True, retry_delay=timedelta(minutes=3))
        def get_ids_from_table(**context):
            """
            Gets the meeting IDs from the 'meetings' table in the database.

            Args:
            ----
              context (dict): The context dictionary containing execution information.

            Returns:
            -------
              list: A list of meeting IDs from the table.
            """
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            engine = hook.get_sqlalchemy_engine()

            return _task_get_ids_from_table(engine)

        @task(provide_context=True, retry_delay=timedelta(minutes=3))
        def save_data_postgres(meeting_ids: list, **context):
            """
            Save data to PostgreSQL.

            Args:
            ----
              meeting_ids (list): A list of meeting IDs to save.
              context (dict): Additional context information.

            Returns:
            -------
              None
            """
            _save_data_postgres(meeting_ids, **context)

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

        _create_table = create_schema_and_table
        _move_file_s3 = move_file_s3()
        _get_ids_from_table = get_ids_from_table()
        check_empty_file() >> [empty_file, _create_table]
        (_create_table >> _get_ids_from_table >> save_data_postgres(_get_ids_from_table) >> _move_file_s3)
        empty_file >> _move_file_s3

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    _extract_data = extract_data()
    start >> minio_tasks() >> _extract_data >> transform() >> load() >> end


etl_meetings()
