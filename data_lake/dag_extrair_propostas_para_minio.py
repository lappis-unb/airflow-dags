"""
Decidim CSV DAG generator

This Airflow DAG is responsible for get proposals data by component id, using the
decidim proposal_hook api and saving it as CSV files to a MinIO bucket.
This DAG is intended to run daily and fetch the updated data.
"""
import os
import yaml
import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
import boto3

from plugins.decidim_hook import DecidimHook
from plugins.graphql.hooks.graphql import GraphQLHook
from plugins.components.proposals import ProposalsHook
from airflow.hooks.base_hook import BaseHook
from pathlib import Path
import numpy as np

DECIDIM_CONN_ID = "api_decidim"

def save_to_minio(data, filename):
    # Fetching MinIO connection details from Airflow connections
    minio_conn = BaseHook.get_connection('minio_connection_id')
    MINIO_URL = minio_conn.host
    MINIO_ACCESS_KEY = minio_conn.login
    MINIO_SECRET_KEY = minio_conn.password
    MINIO_BUCKET = "brasil-participativo-daily-csv"

    # Saving JSON to MinIO bucket using boto3
    s3_client = boto3.client('s3',
                             endpoint_url=MINIO_URL,
                             aws_access_key_id=MINIO_ACCESS_KEY,
                             aws_secret_access_key=MINIO_SECRET_KEY,
                             region_name='us-east-1')
    s3_client.put_object(Body=data,
                         Bucket=MINIO_BUCKET,
                         Key=filename,
                         ContentType='text/csv')

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
    def get_proposals(components_ids, filter_date:datetime):
        """
        Airflow task that uses variable `proposal_hook` to request
        proposals on dedicim API and treats the data.
        """
        proposals_query_directory_path = Path(__file__).parent.joinpath("./queries/get_proposals_by_component_id.gql")
        final_data = None
        for component_id in components_ids:
            component_id = int(component_id)
            logging.info(f"Starting component_id {component_id}")
            proposal_hook = ProposalsHook(DECIDIM_CONN_ID, component_id)
            
            proposals_query = proposal_hook.graphql.get_graphql_query_from_file(proposals_query_directory_path)
            data_normalized = None

            proposals_variables = {'id': component_id, 'filter_date': filter_date}

            data_dict = proposal_hook.graphql.run_graphql_paginated_query(
            proposals_query, COMPONENT_TYPE_TO_EXTRACT, variables=proposals_variables)
            json_data_list = [
                data["data"]["component"]["proposals"]["nodes"]
                for data in data_dict
            ]

            data_normalized = pd.concat(
                [pd.json_normalize(data) for data in json_data_list], 
                axis=0, 
                ignore_index=True
            ) 

            if data_normalized.empty:
                logging.warning(f"Nenhuma proposta cadastrada no dia {filter_date} para o componente {component_id}.")
                continue

            participatory_space = proposal_hook.get_participatory_space()
            if participatory_space["type"] in ["ParticipatoryProcess", "Initiative", "Conference"]:
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
                final_data =  pd.concat([final_data, data_normalized]).reset_index(drop=True)

        csv_string = final_data.sort_values(by="id").to_csv(index=False)
        save_to_minio(csv_string, f"{filter_date}-proposals.csv")

    proposals_ids_task = get_propolsas_components_ids()
    
    get_proposals(proposals_ids_task, exec_date)

decidim_data_extraction()
