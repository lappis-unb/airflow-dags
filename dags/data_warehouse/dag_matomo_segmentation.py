import logging
from datetime import datetime, timedelta
from typing import ClassVar, Dict, List

from io import StringIO
import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from plugins.graphql.hooks.graphql_hook import GraphQLHook
from pathlib import Path

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}
DECIDIM_CONN_ID = "api_decidim"



def _get_query():
    """
    Retrieves the query from the specified file and returns it.

    Returns:
    -------
      str: The query string.
    """
    query = (
        Path(__file__).parent.joinpath("./queries/processes_slug_id.gql").open().read()
    )
    return query

ESPACOS = ["processes"]

@dag(
    default_args=default_args,
    schedule_interval="0 0 * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["matomo", "segmentation"],
)
def dag_matomo_segmentation():

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task(provide_context=True)
    def get_url_matomo(**context):
        slug_id = context["ti"].xcom_pull(task_ids="get_slug_id_teste")
        urls = []
        bp = "https://brasilparticipativo.presidencia.gov.br/processes"
        bp = "https://lab-decide.dataprev.gov.br"
        espaco = "processes"
        for item in slug_id:
            slug = item[0]
            component_id = item[1]
            url = f"{bp}/{espaco}/{slug}/f/{component_id}/"
            urls.append(url)
        return urls


    @task
    def get_segment_matomo():
        matomo_conn = BaseHook.get_connection("matomo_conn")
        matomo_url = matomo_conn.host
        token_auth = matomo_conn.password
        site_id = matomo_conn.login

        params = {
        "module": "API",
        "method": "SegmentEditor.getAll",
        "idSite": site_id,
        "token_auth": token_auth,
        "format": "csv",
        
    }
        response = requests.get(matomo_url, params=params)
        if response.status_code == 200:
            data = StringIO(response.text)
            df = pd.read_csv(data)
            return df['definition'].str.replace("pageUrl=^","").values
        else:
            raise Exception("deu ruim", response.status_code)
    
    @task
    def get_slug_id_teste():
        hook = GraphQLHook(DECIDIM_CONN_ID)
        session = hook.get_session()
        query = _get_query()
        response = session.post(
            hook.api_url,
            json={
                "query": query,
            },
        )
        data = eval(response.text)
        data = data['data']['participatoryProcesses']
        slug_id = []
        for item in data:
            slug = item['slug']
            components = item['components']
            for component in components:
                _id = component['id']
                slug_id.append((slug, _id))
        return slug_id
        
    @task(provide_context=True)
    def filter_url(**context):
        urls = context["ti"].xcom_pull(task_ids="get_url_matomo")
        segments = context["ti"].xcom_pull(task_ids="get_segment_matomo")
        new_segments = set(urls).difference(set(segments))
        return new_segments


    _get_slug_id_teste = get_slug_id_teste()
    _get_url_matomo = get_url_matomo()
    _get_segment_matomo = get_segment_matomo()
    start >> [_get_url_matomo , _get_segment_matomo] >> filter_url() >> end#get_segment_matomo() >> end
    start >> _get_slug_id_teste >> _get_url_matomo
dag = dag_matomo_segmentation()
