"""Decidim CSV DAG generator.

This Airflow DAG is responsible for get proposals data by component id, using the
decidim proposal_hook api and saving it as CSV files to a MinIO bucket.
This DAG is intended to run daily and fetch the updated data.
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path

import boto3
import numpy as np
import pandas as pd
from airflow.decorators import dag, task
from airflow.hooks.base_hook import BaseHook

from plugins.components.proposals import ProposalsHook
from plugins.graphql.hooks.graphql_hook import GraphQLHook
from minio import Minio
import io

DECIDIM_CONN_ID = "api_decidim"
MINIO_CONN_ID = "minio_connection_id"
MINIO_BUCKET = 'brasil-participativo-daily-csv'
COMPONENT_TYPE_TO_EXTRACT = "Proposals"

def _get_minio():    
    """
    Retorna um cliente S3 configurado para se conectar ao MinIO.

    Retorna:
        s3_client (boto3.client): Cliente S3 configurado para se conectar ao MinIO.
    """


    minio_conn = BaseHook.get_connection(MINIO_CONN_ID)
    minio_host = minio_conn.host.split('//')[1]
    minio_access_key = minio_conn.login
    minio_secret_access = minio_conn.password
    minio_bucket = "brasil-participativo-daily-csv"
    print(minio_host, ' to aq ')
    client = Minio(
            minio_host,
            access_key=minio_access_key,
            secret_key=minio_secret_access,
            secure=False,
            )
    return client

def trata_df(df: pd.DataFrame, proposal_hook:ProposalsHook) -> pd.DataFrame:
    """
    Função para processar um DataFrame e adicionar informações de 
    escopo com base no tipo de espaço participativo.

    Parâmetros:
    - df (pd.DataFrame): O DataFrame a ser processado.

    Retorna:
    - pd.DataFrame: O DataFrame processado com informações de escopo adicionadas.
    """
    participatory_space = proposal_hook.get_participatory_space()
    if participatory_space["type"] in [
        "ParticipatoryProcess",
        "Initiative",
        "Conference",
    ]:
        scope = proposal_hook.get_participatory_escope()

        df["scope_id"] = scope["scope"]["id"]
        df["scope"] = scope["scope"]["name"]["translation"]
    else:
        df["scope_id"] = " "
        df["scope"] = " "
    return df

@dag(
    default_args={
        "owner": "Thais R.",
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
    },
    schedule="0 23 * * *",
    catchup=False,
    start_date=datetime(2023, 11, 10),
    description=__doc__,
    tags=["decidim", "minio"],
)
def decidim_data_extraction():
    exec_date = "{{ yesterday_ds }}"

    @task
    def get_propolsas_components_ids():
        all_components = GraphQLHook(DECIDIM_CONN_ID).get_components_ids_by_type(COMPONENT_TYPE_TO_EXTRACT)
        return all_components

    @task
    def get_proposals(components_ids, filter_date: datetime):
        """
        Tarefa do Airflow que utiliza `proposal_hook` para requisitar propostas na API e tratar os dados.

        Args:
        ----
            components_ids (list): Lista de IDs de componentes para os quais as propostas serão obtidas.
            filter_date (datetime): Data para filtrar as propostas.

        Raises:
        ------
            Warning: Se nenhuma proposta estiver cadastrada na data especificada.

        Returns:
        -------
            None

        Obs:
        ----
            A função utiliza a classe `ProposalsHook` para interagir com a API do Dedicim.
        """
        proposals_query_directory_path = Path(__file__).parent.joinpath(
            "./queries/get_proposals_by_component_id.gql"
        )
        final_data = None
        dfs = []
        for component_id in components_ids:
            component_id = int(component_id)
            logging.info("Starting component_id %s.", component_id)
            proposal_hook = ProposalsHook(DECIDIM_CONN_ID, component_id)

            proposals_query = proposal_hook.graphql.get_graphql_query_from_file(
                proposals_query_directory_path
            )
            proposals_variables = {"id": component_id, "date": filter_date}
            data_dict = proposal_hook.graphql.run_graphql_paginated_query(
                proposals_query,
                variables=proposals_variables,
            )
            json_data_list = [data["data"]["component"]["proposals"]["nodes"] for data in data_dict]
            for json_data in json_data_list:
                df = pd.json_normalize(json_data)
                if len(df) == 0:
                    continue
                df = trata_df(df, proposal_hook)
                dfs.append(df)

        return pd.concat(dfs)

    @task.branch(provide_context=True)
    def verifica_bucket():
        minio = _get_minio()
        if minio.bucket_exists(MINIO_BUCKET):
            return 'salva_no_minio'
        return 'cria_bucket'

    @task(provide_context=True, trigger_rule='none_failed_min_one_success' )
    def salva_no_minio(**context):
        task_instance = context['task_instance']
        df = task_instance.xcom_pull(task_ids='get_proposals')

        csv_data = df.to_csv(index=False)
        data = io.BytesIO(csv_data.encode())

        minio = _get_minio()
        minio.put_object(
            bucket_name=MINIO_BUCKET,
            object_name='teste.csv',
            data=data,
            length=data.getbuffer().nbytes)

    @task
    def cria_bucket():
        minio = _get_minio()
        minio.make_bucket(MINIO_BUCKET, object_lock=False)

    _get_propolsas_components_ids = get_propolsas_components_ids()
    _get_proposals = get_proposals(_get_propolsas_components_ids, '{{ ds_nodash }}')
    
    _save_no_minio = salva_no_minio()
    _cria_bucket = cria_bucket()
    _verifica_bucket = verifica_bucket()

    _get_proposals >> _verifica_bucket
    _verifica_bucket >> [_save_no_minio, _cria_bucket]
    _cria_bucket >> _save_no_minio

decidim_data_extraction()
