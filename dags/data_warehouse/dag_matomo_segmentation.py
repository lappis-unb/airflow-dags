import logging
from datetime import datetime, timedelta
from io import StringIO
from typing import List, Tuple

import pandas as pd
import requests
from airflow.decorators import dag, task, task_group
from airflow.hooks.base_hook import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from data_warehouse.tools import add_temporal_columns
from airflow.hooks.postgres_hook import PostgresHook
default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}
DECIDIM_CONN_ID = "api_decidim"
POSTGRES_CONN_ID = "conn_postgres"


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
        segmentations = df["definition"].str.replace("pageUrl=^", "").values.tolist()
        return segmentations
    except requests.exceptions.RequestException as e:
        logging.error("Erro na solicitação HTTP: %s", e)
        raise e


def get_credentials_matomo():
    matomo_conn = BaseHook.get_connection("matomo_conn")
    matomo_url = matomo_conn.host
    token_auth = matomo_conn.password
    site_id = matomo_conn.login
    return matomo_url, token_auth, site_id


methods = [
    ("VisitsSummary", "get"),
    ("Actions", "getPageUrls"),
    ("Actions", "getPageTitles"),
    ("Actions", "getOutlinks"),
    ("Referrers", "getAll"),
    ("UserCountry", "getCountry"),
    ("UserCountry", "getRegion"),
]


def _task_get_data_matomo(all_segmentos, method, space, **context):
    matomo_url, token_auth, site_id = get_credentials_matomo()
    url_matomo = f"https://brasilparticipativo.presidencia.gov.br/{space}/"
    segmentos = [segmento for segmento in all_segmentos if segmento.startswith(url_matomo)]
    
    results = {}
    date = context["execution_date"].strftime("%Y-%m-%d")

    for segmento in segmentos:
        params = {
            "module": "API",
            "idSite": site_id,
            "period": "day",
            "date": date,
            "format": "csv",
            "token_auth": token_auth,
            "segment": f"pageUrl=={segmento}",
            "method": f"{method[0]}.{method[1]}",
        }
        response = requests.get(matomo_url, params=params)

        dfs = []
        try:
            response.raise_for_status()
            results[segmento] = response.text
            data = StringIO(response.text)
            df = pd.read_csv(data)
            df["space"] = space
            df["segment"] = segmento
            df["method"] = method[1]
            df["date"] = date
            dfs.append(df)

        except requests.exceptions.RequestException:
            logging.error("Erro na solicitação, segmento: %s", segmento)
    return pd.concat(dfs)


MINIO_CONN = "minio_conn_id"
components = ["assemblies", "processes"]
BUCKET_NAME = "teste-bucket"

def get_file_name(space: str, method: Tuple[str, str], context: dict) -> str:
    name_file = f"{space}/{context['ds_nodash']}/{method[1]}_{method[0]}.json"
    return name_file


@dag(
    default_args=default_args,
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

    segmento = get_segment_matomo()
    start >> segmento
    get_dados_list = []

    for space in components:

        @task_group(group_id=space)
        def group(space):
            for method in methods:

                @task(task_id=f"save_minio_{method[1]}_{method[0]}", pool="matomo_pool", provide_context=True)
                def get_data_matomo(segmentos: List[str], space: str, method: tuple, **context):
                    data = _task_get_data_matomo(segmentos, method, space, **context)
                    data = add_temporal_columns(data, context['execution_date'])

                    filename = get_file_name(space, method, context)
                    S3Hook(aws_conn_id=MINIO_CONN).load_string(
                        string_data=data.to_json(orient="records"),
                        key=filename,
                        bucket_name=BUCKET_NAME,
                        replace=True,
                    )

# 
                @task(task_id=f"save_postgres_{method[1]}_{method[0]}", provide_context=True)
                def _task_save_postgres(space, method, **context):
                    filename = get_file_name(space, method, context)
                    
                    key = S3Hook(aws_conn_id=MINIO_CONN).read_key(
                        key=filename,
                        bucket_name=BUCKET_NAME,
                    )
                    df = pd.read_json(key)
                    engine = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_sqlalchemy_engine()
                    df.to_sql(
                        name=f"teste",
                        con=engine,
                        if_exists="append",
                        index=False,
                        schema='raw',
                    )
                    return df

                get_data_matomo(segmento, space, method) >> _task_save_postgres(space, method)

        group(space=space) >> end
        # group(space) >> end

    

components_matomo()
