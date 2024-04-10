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
from typing import List, Tuple, Dict


default_args = {
    "owner": "Amoêdo",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
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

def _extract_id_date_from_response(response:str) -> pd.DataFrame:
    """
    Extracts the id and date information from the given response.

    Args:
        response (str): The response string containing the data.

    Returns:
        pd.DataFrame: A DataFrame containing the extracted id and date information.
    """
    proposals_lists = []
    for process in response['data']['participatoryProcesses']:
        components = process.get('components')
        nodes_lists = [component.get('proposals', {}).get('nodes') or [] for component in components]

        for nodes in nodes_lists:
            proposals_lists.append(nodes)
    df_ids = pd.concat(pd.json_normalize(i) for i in proposals_lists)
    return df_ids

def _get_response_gql(query:str, response_text:bool=False, **variables):
    """
    Executes the GraphQL query to get the date and id of the update proposals.

    Parameters:
    ----------
    query : str
        The GraphQL query to be executed.

    Returns:
    -------
    dict
        The response from the GraphQL query.
    """
    hook = GraphQLHook(DECIDIM_CONN_ID)
    session = hook.get_session()
    response = session.post(
        hook.api_url,
        json={
            "query": query,
            "variables": variables,
        },
    )
    dado = response.text
    if response_text:
        return dado
    return json.loads(dado)

def _filter_ids_by_ds_nodash(ids: pd.DataFrame, date: str) -> pd.DataFrame:
    """
    Filter the given DataFrame based on the provided date.

    Args:
        ids (pd.DataFrame): The DataFrame containing the IDs and updated dates.
        date (str): The date to filter the DataFrame by.

    Returns:
        pd.DataFrame: The filtered DataFrame containing only the rows with the specified date.
    """
    print(ids.columns, 'to aq')
    ids = ids[ids['updatedAt']
        .apply(lambda x: x[:10]
               .replace('-', '')) == date]
    return list(ids['id'].values)

def collect_responses(ids:List[str]):
    s3 = S3Hook(MINIO_CONN)
    responses = []
    for _id in ids:
        response = s3.read_key(f'updated_proposals/landing_zone/{_id}.json', MINIO_BUCKET)
        response = json.loads(response)
        responses.append(response)
    return responses

QUERY = _get_query()
DECIDIM_CONN_ID = "api_decidim"
MINIO_CONN = 'minio_conn_id'
MINIO_BUCKET = 'brasil-participativo-daily-csv'


@dag(
    default_args=default_args,
    schedule_interval="0 22 * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["data_lake"],
)
def ingest_update_proposals():
    start = EmptyOperator(task_id="start")
    
    @task
    def get_date_id_update_proposals():
        query = _get_query()
        response = _get_response_gql(query)
        response = _extract_id_date_from_response(response)
        return response
    
    @task(provide_context=True)
    def get_current_updated_ids(ids:pd.DataFrame, **context):
        ids = _filter_ids_by_ds_nodash(ids, context['ds_nodash'])
        return ids

    @task        
    def check_and_create_bucket():
        hook = S3Hook(MINIO_CONN)
        if not hook.check_for_bucket(MINIO_BUCKET):
            hook.create_bucket(bucket_name=MINIO_BUCKET)

    @task
    def get_updated_proposals(ids:List[str]):
        query = _get_query('./queries/get_proposals_by_id.gql')
        print(query)
        for _id in ids:
            response = _get_response_gql(query, response_text=True, id=_id)
            print(response)
            hook = S3Hook(MINIO_CONN)
            hook.load_string(response, key=f'updated_proposals/landing_zone/{_id}.json', bucket_name=MINIO_BUCKET, replace=True)

    @task(provide_context=True)
    def transform_updated_proposals(**context):
        ids = context['task_instance'].xcom_pull(task_ids='get_current_updated_ids')
        responses = collect_responses(ids)
        print(responses[0])
    
    _get_date_id_update_proposals = get_date_id_update_proposals()
    _get_current_updated_ids= get_current_updated_ids(_get_date_id_update_proposals)
    _get_updated_proposals = get_updated_proposals(_get_current_updated_ids)
    _check_and_create_bucket = check_and_create_bucket()
    _get_current_updated_ids >> _check_and_create_bucket >> _get_updated_proposals
    
    _get_updated_proposals >> transform_updated_proposals()

    start >> _get_date_id_update_proposals

dag = ingest_update_proposals()