import json
import logging
from datetime import datetime, timedelta
from io import StringIO
from typing import List, Tuple

import pandas as pd
import requests
from airflow.datasets import Dataset
from airflow.decorators import dag, task, task_group
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
)
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from data_warehouse.tools import add_temporal_columns
from inflection import underscore

from plugins.matomo.hook import MatomoHook


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def _task_get_segment_matomo():
    """
    Retrieves segment definitions from Matomo API.

    Returns:
    -------
        pandas.Series: A series containing the segment definitions.
    Raises:
    ------
        Exception: If the API request fails.
    """
    matomo_hook = MatomoHook(MATOMO_CONN_ID)

    filters = {"filter_limit": -1}

    try:
        response = matomo_hook.secure_request(
            module="SegmentEditor", method="getAll", filters=filters, response_format="csv"
        )
        data = StringIO(response)
        df = pd.read_csv(data)
        segments = df["definition"].str.replace("pageUrl=^", "").values.tolist()
        return segments
    except requests.exceptions.RequestException as e:
        logging.error("Erro na solicitação HTTP: %s", e)
        raise e


def _task_get_data_matomo(all_segments, method, space, period, **context):
    """
    Get data from Matomo API for the specified segments.

    Args:
    ----
        all_segmentos (list): List of segments to retrieve data for.
        method (tuple): Tuple representing the Matomo API method to use.
        space (str): Space identifier.
        **context: Additional context variables passed by Airflow.

    Returns:
    -------
        pandas.DataFrame: Concatenated DataFrame containing the data for all segments.
    """
    matomo_hook = MatomoHook(MATOMO_CONN_ID)
    segments = _get_segments(all_segments, space)
    method = f"{method[0]}.{method[1]}"
    date = context["execution_date"].strftime("%Y-%m-%d")

    dfs = []
    bulk_segments = [
        {
            "module": "API",
            "period": period,
            "date": date,
            "segment": f"pageUrl=={segment}",
            "method": method,
        }
        for segment in segments
    ]
    for bulk_segment in chunks(bulk_segments, 10):
        """
        Using matomo bulk request there are 3 possible results:
            1. List with dicts
            2. List with list of dicts
            3. Empty list
        The first case happens when there is only one line per segment for the requested method.
        The second case happens when there is multiple lines per segment for the requested method.
        The third case happens when there is no data availeble.
        """
        response = matomo_hook.secure_bulk_request(bulk_segment, response_format="json")
        data_list: list[dict | list[dict]] = json.loads(response)
        current_segments = [item["segment"] for item in bulk_segment]

        if all([isinstance(i, dict) for i in data_list]):
            df = _transform_response_to_df(
                data=data_list, method=method, space=space, date=date, segment=current_segments
            )
            dfs.append(df)
        elif all([isinstance(i, list) for i in data_list]):
            for item, segment in zip(data_list, current_segments):
                df = _transform_response_to_df(
                    data=item, method=method, space=space, date=date, segment=segment
                )
                dfs.append(df)
        else:
            raise Exception

    df = pd.concat(dfs, ignore_index=True)
    return df


def _transform_response_to_df(
    data: list[dict], method: tuple, space: str, date: str, segment: str | list[str]
) -> pd.DataFrame:
    """
    Transforms the response from an API call into a pandas DataFrame.

    Args:
    ----
        method (tuple): The HTTP method used for the API call.
        space (str): The space value.
        date (str): The date value.
        segmento (str): The segmento value.
        response (dict): The response from the API call.

    Returns:
    -------
        pd.DataFrame: The transformed data as a pandas DataFrame.
    """
    df = pd.DataFrame(data)
    df["space"] = space
    df["method"] = method
    df["date"] = date
    df["url"] = segment

    return df


def _get_segments(all_segments, space):
    """
    Filter the given list of segments based on the provided space.

    Args:
    ----
        all_segmentos (list): A list of segments.
        space (str): The space to filter the segments.

    Returns:
    -------
        list: A list of segments that start with the specified space.
    """
    url_matomo = f"https://brasilparticipativo.presidencia.gov.br/{space}/"
    segments: List[str] = [segment for segment in all_segments if segment.startswith(url_matomo)]
    return segments


def _get_file_name(space: str, method: Tuple[str, str], context: dict) -> str:
    """
    Constructs and returns the file name based on the given parameters.

    Args:
    ----
        space (str): The space value.
        method (Tuple[str, str]): A tuple containing two strings representing the method.
        context (dict): A dictionary containing the context information.

    Returns:
    -------
        str: The constructed file name.

    """
    name_file = f"{space}/{context['ds_nodash']}/{method[1]}_{method[0]}.json"
    return name_file


def _save_df_postgres(method, df):
    """
    Save a DataFrame to a PostgreSQL table.

    Args:
    ----
        method (tuple): A tuple containing the method information.
        df (pandas.DataFrame): The DataFrame to be saved.

    Returns:
    -------
        None
    """
    if df.empty:
        logging.warning("DataFrame vazio, não será salvo no PostgreSQL.")
        return

    engine = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_sqlalchemy_engine()
    name_table = f"{underscore(method[0])}_{underscore(method[1])}".lower()
    df.to_sql(
        name=name_table,
        con=engine,
        if_exists="append",
        index=False,
        schema=SCHEMA,
    )
    logging.info("Dados %d salvos na tabela %s.%s", len(df), SCHEMA, name_table)


def _get_df_from_minio(filename):
    """
    Retrieves a DataFrame from Minio storage.

    Args:
    ----
        filename (str): The name of the file to retrieve from Minio.

    Returns:
    -------
        pandas.DataFrame: The DataFrame read from the file.

    """
    key = S3Hook(aws_conn_id=MINIO_CONN).read_key(
        key=f"{MATOMO_PATH}/{filename}",
        bucket_name=BUCKET_NAME,
    )
    df = pd.read_json(key)
    return df


def add_period(df, period):
    """
    Adds a period column to the DataFrame.

    Args:
    ----
        df (pandas.DataFrame): The DataFrame to add the period column to.
        period (str): The period to add to the DataFrame.

    Returns:
    -------
        pandas.DataFrame: The DataFrame with the period column added.

    """
    df["period"] = period
    return df


# Constantes

METHODS = [
    ("VisitsSummary", "get"),
    ("VisitFrequency", "get"),
    ("UserCountry", "getRegion"),
    ("UserCountry", "getCountry"),
    ("DevicesDetection", "getType"),
]

DEFAULT_ARGS = {
    "owner": "Amoedo/Paulo",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}
DECIDIM_CONN_ID = "api_decidim"
POSTGRES_CONN_ID = "conn_postgres"
MATOMO_CONN_ID = "matomo_conn"
MINIO_CONN = "minio_conn_id"
MATOMO_PATH = "dag_components_matomo"
BUCKET_NAME = "brasil-participativo"
SCHEMA = "raw"
SPACES = ["assemblies", "processes"]
TIMEOUT = 60  # 60 segundos
SCHEDULER_INTERVALS = {
    "day": "@daily",
    "week": "@weekly",
    "month": "@monthly",
}


def dag_generator(period, scheduler_interval):
    @dag(
        default_args=DEFAULT_ARGS,
        schedule_interval=scheduler_interval,
        start_date=datetime(2023, 1, 1),
        catchup=True,
        tags=["matomo", "segmentation"],
        dag_id=f"components_matomo_{period}",
    )
    def components_matomo():
        """
        Airflow DAG for performing data segmentation with Matomo.

        This DAG retrieves data from Matomo for different segments, spaces, and methods.
        It saves the data as JSON files in a MinIO bucket and also saves the data to PostgreSQL.

        The DAG consists of the following tasks:
        - start: Dummy task to start the DAG.
        - end: Dummy task to end the DAG.
        - create_bucket: Task to create a bucket in MinIO.
        - create_schema: Task to create a schema in PostgreSQL.
        - get_segment_matomo: Task to retrieve the segment data from Matomo.
        - group (task group): Task group for processing data for each space.
            - get_data_matomo: Task to retrieve and save data to MinIO for each method.
            - task_save_postgres: Task to save data to PostgreSQL for each method.

        The tasks are connected in the following order:
        start >> [create_bucket, create_schema] >> segment >> group >> end
        """
        start = EmptyOperator(task_id="start")
        end = EmptyOperator(task_id="end")

        create_bucket = S3CreateBucketOperator(
            task_id="create_bucket",
            bucket_name=BUCKET_NAME,
            aws_conn_id=MINIO_CONN,
        )

        create_schema = SQLExecuteQueryOperator(
            task_id="create_schema",
            sql=f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};",
            conn_id=POSTGRES_CONN_ID,
        )

        @task
        def get_segment_matomo():
            return _task_get_segment_matomo()

        segment = get_segment_matomo()
        start >> [create_bucket, create_schema] >> segment

        for space in SPACES:

            @task_group(group_id=space)
            def group(space):
                for method in METHODS:

                    @task(
                        task_id=f"save_minio_{underscore(method[0])}_{underscore(method[1])}".lower(),
                        pool="matomo_pool",
                        provide_context=True,
                    )
                    def get_data_matomo(segments: List[str], space: str, method: tuple, period, **context):
                        """
                        Retrieves data from Matomo for the given segments, space, and method.

                        Adds temporal columns to the data based on the execution date.
                        Saves the data as a JSON file in a MinIO bucket.

                        Args:
                        ----
                            segments (List[str]): List of segment names.
                            space (str): Space name.
                            method (tuple): Tuple containing method information.
                            **context: Additional context passed by Airflow.

                        Returns:
                        -------
                            None
                        """
                        data = _task_get_data_matomo(segments, method, space, period, **context)
                        data = add_temporal_columns(data, context["execution_date"])
                        data = add_period(data, period)
                        filename = _get_file_name(space, method, context)
                        S3Hook(aws_conn_id=MINIO_CONN).load_string(
                            string_data=data.to_json(orient="records"),
                            key=f"{MATOMO_PATH}/{filename}",
                            bucket_name=BUCKET_NAME,
                            replace=True,
                        )

                    @task(
                        task_id=f"save_postgres_{underscore(method[0])}_{underscore(method[1])}".lower(),
                        provide_context=True,
                        outlets=[Dataset(f"{underscore(method[0])}_{underscore(method[1])}".lower())],
                    )
                    def task_save_postgres(space, method, **context):
                        """
                        Task to save data to PostgreSQL.

                        Args:
                        ----
                            space (str): The space parameter.
                            method (str): The method parameter.
                            **context: Additional context parameters.
                        """
                        filename: str = _get_file_name(space, method, context)
                        df: pd.DataFrame = _get_df_from_minio(filename)
                        _save_df_postgres(method, df)

                    get_data_matomo(segment, space, method, period) >> task_save_postgres(space, method)

            group(space=space) >> end

    return components_matomo()


for key, value in SCHEDULER_INTERVALS.items():
    dag_generator(key, value)
