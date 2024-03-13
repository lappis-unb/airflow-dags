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

DECIDIM_CONN_ID = "api_decidim"


def save_to_minio(data, filename):
    # Fetching MinIO connection details from Airflow connections
    minio_conn = BaseHook.get_connection("minio_connection_id")
    minio_conn = minio_conn.host
    minio_access_key = minio_conn.login
    minio_secret_access = minio_conn.password
    minio_bucket = "brasil-participativo-daily-csv"

    # Saving JSON to MinIO bucket using boto3
    s3_client = boto3.client(
        "s3",
        endpoint_url=minio_conn,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_access,
        region_name="us-east-1",
    )
    s3_client.put_object(Body=data, Bucket=minio_bucket, Key=filename, ContentType="text/csv")


COMPONENT_TYPE_TO_EXTRACT = "Proposals"


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
        for component_id in components_ids:
            component_id = int(component_id)
            logging.info("Starting component_id %s.", component_id)
            proposal_hook = ProposalsHook(DECIDIM_CONN_ID, component_id)

            proposals_query = proposal_hook.graphql.get_graphql_query_from_file(
                proposals_query_directory_path
            )
            data_normalized = None

            proposals_variables = {"id": component_id, "date": filter_date}

            data_dict = proposal_hook.graphql.run_graphql_paginated_query(
                proposals_query,
                variables=proposals_variables,
            )

            json_data_list = [data["data"]["component"]["proposals"]["nodes"] for data in data_dict]

            data_normalized = pd.concat(
                [pd.json_normalize(data) for data in json_data_list],
                axis=0,
                ignore_index=True,
            )

            if data_normalized.empty:
                logging.warning("Nenhuma proposta cadastrada no dia %s.", filter_date)
                continue

            participatory_space = proposal_hook.get_participatory_space()
            if participatory_space["type"] in [
                "ParticipatoryProcess",
                "Initiative",
                "Conference",
            ]:
                scope = proposal_hook.get_participatory_escope()

                data_normalized["scope_id"] = scope["scope"]["id"]
                data_normalized["scope"] = scope["scope"]["name"]["translation"]
            else:
                data_normalized["scope_id"] = " "
                data_normalized["scope"] = " "

            data_normalized["participatory.space.title"] = participatory_space["title"]["translation"]

            link_base = proposal_hook.get_component_link()

            ids = np.char.array(data_normalized["id"].values, unicode=True)
            data_normalized = data_normalized.assign(link=(link_base + "/" + ids).astype(str))
            if final_data is None:
                final_data = data_normalized
            elif isinstance(final_data, pd.DataFrame):
                final_data = pd.concat([final_data, data_normalized]).reset_index(drop=True)

        csv_string = final_data.sort_values(by="id").to_csv(index=False)
        save_to_minio(csv_string, f"{filter_date}-proposals.csv")

    @task
    def get_proposals_commments(components_ids, filter_date: datetime):
        """
        Tarefa do Airflow que solicita os comentários de cada proposta da API e processa os dados.

        Args:
        ----
            ids_componentes (list): Lista de IDs de componentes.
            data_filtro (datetime): Data das propostas a serem pesquisadas.

        Returns:
        -------
            None

        Obs:
        ---
            A função utiliza a classe `ProposalsHook` para interagir com a API do Dedicim.
        """
        result_df = None

        for component_id in components_ids:
            hook = ProposalsHook(DECIDIM_CONN_ID, component_id)
            current_comments = hook.get_comments(update_date_filter=filter_date)
            if current_comments is None:
                continue

            if result_df is None:
                result_df = pd.DataFrame.from_records(current_comments)
            else:
                result_df = pd.concat([result_df, pd.DataFrame.from_records(current_comments)], axis=0)

        if result_df is None:  # Faz sentido não guardar nenhum dado ?
            logging.warning("Nenhum comentario feito no dia %s.", filter_date)
            return None

        csv_string = result_df.to_csv(index=False)
        save_to_minio(csv_string, f"{filter_date}-comments-in-proposals.csv")

    proposals_ids_task = get_propolsas_components_ids()

    get_proposals(proposals_ids_task, exec_date)


decidim_data_extraction()
