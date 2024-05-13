import logging
from datetime import datetime, timedelta
from io import StringIO
from typing import List, Tuple

import pandas as pd
import requests
from airflow.decorators import dag, task, task_group
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from data_warehouse.tools import add_temporal_columns


def get_credentials_matomo(matomo_conn: str = "matomo_conn"):
    """
    Retrieves the credentials required to connect to the Matomo API.

    Args:
    ----
        matomo_conn (str): The name of the Airflow connection for Matomo.

    Returns:
    -------
        tuple: A tuple containing the Matomo URL, token authentication, and site ID.
    """
    matomo_conn = BaseHook.get_connection(matomo_conn)
    matomo_url = matomo_conn.host
    token_auth = matomo_conn.password
    site_id = matomo_conn.login
    return matomo_url, token_auth, site_id


spaces = {"processes": "participatoryProcesses", "assemblies": "assemblies"}


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
    matomo_url, token_auth, site_id = get_credentials_matomo()

    params = {
        "module": "API",
        "method": "SegmentEditor.getAll",
        "idSite": site_id,
        "token_auth": token_auth,
        "format": "csv",
        "filter_limit": "-1",
    }
    try:
        response = requests.get(matomo_url, params=params)
        response.raise_for_status()
        data = StringIO(response.text)
        df = pd.read_csv(data)
        segments = df["definition"].str.replace("pageUrl=^", "").values.tolist()
        return segments
    except requests.exceptions.RequestException as e:
        logging.error("Erro na solicitação HTTP: %s", e)
        raise e


def _task_get_data_matomo(all_segments, method, space, **context):
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
    matomo_url, token_auth, site_id = get_credentials_matomo()
    segments = _get_segments(all_segments, space)
    date = context["execution_date"].strftime("%Y-%m-%d")

    dfs = []
    for segment in segments:
        params = {
            "module": "API",
            "idSite": site_id,
            "period": "day",
            "date": date,
            "format": "csv",
            "token_auth": token_auth,
            "segment": f"pageUrl=={segment}",
            "method": f"{method[0]}.{method[1]}",
        }
        response = requests.get(matomo_url, params=params)
        if not _verify_response(segment, response):
            continue

        df = _transform_response_to_df(method, space, date, segment, response)
        dfs.append(df)
    return pd.concat(dfs)


def _transform_response_to_df(
    method: tuple, space: str, date: str, segment: str, response: dict
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
    data = StringIO(response.text)
    df = pd.read_csv(data)
    df["space"] = space
    df["url"] = segment
    df["method"] = method[1]
    df["date"] = date
    return df


def _verify_response(segment, response):
    """
    Verify the response status of a request.

    Args:
    ----
        segmento (str): The segment name.
        response (requests.Response): The response object.

    Returns:
    -------
        bool: True if the response status is successful, False otherwise.
    """
    try:
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException:
        logging.error("Erro na solicitação, segmento: %s", segment)
        return False


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


def _save_df_postgres(space, method, df):
    """
    Save a DataFrame to a PostgreSQL table.

    Args:
    ----
        space (str): The space name.
        method (tuple): A tuple containing the method information.
        df (pandas.DataFrame): The DataFrame to be saved.

    Returns:
    -------
        None
    """
    engine = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_sqlalchemy_engine()
    name_table = f"{space}_{method[1]}_{method[0]}".lower()
    df.to_sql(
        name=name_table,
        con=engine,
        if_exists="append",
        index=False,
        schema=SCHEMA,
    )


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
        key=filename,
        bucket_name=BUCKET_NAME,
    )
    df = pd.read_json(key)
    return df


METHODS = [
    ("VisitsSummary", "get"),
    ("Actions", "getPageUrls"),
    ("Actions", "getPageTitles"),
    ("Actions", "getOutlinks"),
    ("Referrers", "getAll"),
    ("UserCountry", "getCountry"),
    ("UserCountry", "getRegion"),
]

DEFAULT_ARGS = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}
DECIDIM_CONN_ID = "api_decidim"
POSTGRES_CONN_ID = "conn_postgres"
MINIO_CONN = "minio_conn_id"
SPACES = ["assemblies", "processes"]
BUCKET_NAME = "teste-bucket"
SCHEMA = "raw"


@dag(
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["matomo", "segmentation"],
)
def components_matomo():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task
    def get_segment_matomo():
        return _task_get_segment_matomo()

    segment = get_segment_matomo()
    start >> segment

    for space in SPACES:

        @task_group(group_id=space)
        def group(space):
            for method in METHODS:

                @task(task_id=f"save_minio_{method[1]}_{method[0]}", pool="matomo_pool", provide_context=True)
                def get_data_matomo(segments: List[str], space: str, method: tuple, **context):
                    """
                    Retrieves data from Matomo for the given segmentos, space, and method.

                    Adds temporal columns to the data based on the execution date.
                    Saves the data as a JSON file in a MinIO bucket.

                    Args:
                    ----
                        segmentos (List[str]): List of segment names.
                        space (str): Space name.
                        method (tuple): Tuple containing method information.
                        **context: Additional context passed by Airflow.

                    Returns:
                    -------
                        None
                    """
                    data = _task_get_data_matomo(segments, method, space, **context)
                    data = add_temporal_columns(data, context["execution_date"])
                    filename = _get_file_name(space, method, context)
                    S3Hook(aws_conn_id=MINIO_CONN).load_string(
                        string_data=data.to_json(orient="records"),
                        key=filename,
                        bucket_name=BUCKET_NAME,
                        replace=True,
                    )

                @task(task_id=f"save_postgres_{method[1]}_{method[0]}", provide_context=True)
                def _task_save_postgres(space, method, **context):
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
                    _save_df_postgres(space, method, df)

                get_data_matomo(segment, space, method) >> _task_save_postgres(space, method)

        group(space=space) >> end


components_matomo()
