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
from typing import List, Tuple

default_args = {
    "owner": "Amoêdo",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}


def _get_query(relative_path:str="./queries/get_updat_at_proposals.gql"):
    """
    Retrieves the query from the specified file and returns it.

    Returns:
    -------
      str: The query string.
    """
    query = (
        Path(__file__).parent.joinpath(relative_path).open().read()
    )
    return query

def _extract_id_date_from_response(response:str) -> List[Tuple]:
    results = []

    for data in full_data:
        results = []
        full_data = response['data']['participatoryProcesses']
        for data in full_data:
            components = data['components']
            for component in components:
                # Verificar se 'proposals' está presente no componente
                if 'proposals' in component:
                    # Iterar sobre cada nó em 'proposals'
                    for node in component['proposals']['nodes']:
                        _id, date = node['id'], node['updatedAt']
                        date = datetime.strptime(date[:10], '%Y-%m-%d')
                        result = _id, date
                        results.append(result)

    return results

def _task_get_date_id_update_proposals(query:str) -> str:
    """
    Executes the GraphQL query to get the date and id of the update proposals.

    Returns:
    -------
      dict: The response from the GraphQL query.
    """
    hook = GraphQLHook(DECIDIM_CONN_ID)
    session = hook.get_session()
    response = session.post(
        hook.api_url,
        json={
            "query": query,
        },
    )
    dado = response.text
    return dado

QUERY = _get_query()
DECIDIM_CONN_ID = "api_decidim"


@dag(
    default_args=default_args,
    schedule_interval="0 22 * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    retries=0,
    tags=["data_lake"],
)
def ingest_update_proposals():
    start = EmptyOperator(task_id="start")
    
    @task
    def get_date_id_update_proposals():
        query = _get_query()
        response = _task_get_date_id_update_proposals(query)
        response = _extract_id_date_from_response(response)
        return response
    
    start >> get_date_id_update_proposals()

dag = ingest_update_proposals()