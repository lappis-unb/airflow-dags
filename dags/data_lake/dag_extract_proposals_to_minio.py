import io
import json
from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task
from airflow.hooks.base_hook import BaseHook
from airflow.operators.empty import EmptyOperator
from minio import Minio

from plugins.graphql.hooks.graphql_hook import GraphQLHook

# Vai ser trocado para salvar em arquivo
QUERY = """
query teste ($start_date: String!, $end_date: String!) {
  participatoryProcesses  {
    title{
      translations{
        text
      }
    }
    components{
      id
      ... on Proposals   {
        __typename
        name {
          translations {
            text
          }
        }
        proposals (filter:  {publishedSince: $start_date, publishedBefore: $end_date}  ) {
          nodes {
            id
            createdAt
            publishedAt
            updatedAt
            attachments{
              thumbnail
              type
              url
            }
            author{
              id
              name
              nickname
              organizationName
            }
            body{
              translations{
                text
              }
            }
            category{
              id
              name{
                translations{
                  text
                }
              }
            }

            authorsCount
            userAllowedToComment
            endorsementsCount
            totalCommentsCount
            versionsCount
            voteCount
            commentsHaveAlignment
            commentsHaveVotes
            createdInMeeting
            hasComments
            official
            fingerprint{
              source
              value
            }
            position
            reference
            scope{
              id
              name{
                translations{
                  text
                }
              }
            }
            state
            title{
              translations{
                text
              }
            }
          }
        }
      }
    }
  }
}


"""
DECIDIM_CONN_ID = "api_decidim"
MINIO_CONN_ID = "minio_connection_id"
MINIO_BUCKET = "brasil-participativo-daily-csv"
COMPONENT_TYPE_TO_EXTRACT = "Proposals"

LANDING_ZONE_FILE_NAME = "landing_zone/proposals{date_file}.json"
PROCESSED_FILE_NAME = "processed/proposals{date_file}.csv"


def flatten_structure_with_additional_fields(data):
    """
    Flattens the nested structure of the input data and.

    extracts additional fields for each proposal.

    Args:
    ----
      data (dict): The input data containing nested structure.

    Returns:
    -------
      list: A list of dictionaries, where each dictionary
      represents a flattened proposal with additional fields.

    """
    data = data["data"]["participatoryProcesses"]

    # Function to handle the extraction of text from nested translation dictionaries
    def extract_text(translations):
        if translations and isinstance(translations, list):
            return translations[0].get("text")

    flattened_data = []
    for item in data:
        main_title = extract_text(item.get("title", {}).get("translations", []))
        for component in item.get("components", []):
            component_id = component.get("id", "")
            component_name = extract_text(component.get("name", {}).get("translations", []))
        if "proposals" in component:
            for proposal in component.get("proposals", {}).get("nodes", []):
                proposal_data = {
                    "main_title": main_title,
                    "component_id": component_id,
                    "component_name": component_name,
                    "proposal_id": proposal["id"],
                    "proposal_createdAt": proposal["createdAt"],
                    "proposal_publishedAt": proposal.get("publishedAt"),
                    "proposal_updatedAt": proposal.get("updatedAt"),
                    "author_name": dict_safe_get(proposal, "author").get("name"),
                    "author_nickname": dict_safe_get(proposal, "author").get("nickname"),
                    "author_organization": dict_safe_get(proposal, "author").get("organizationName"),
                    "proposal_body": extract_text(proposal.get("body", {}).get("translations", [])),
                    "category_name": extract_text(
                        dict_safe_get(dict_safe_get(proposal, "category"), "name").get("translations", [])
                    ),
                    "proposal_title": extract_text(proposal.get("title", {}).get("translations", [])),
                    "authorsCount": proposal.get("authorsCount"),
                    "userAllowedToComment": proposal.get("userAllowedToComment"),
                    "endorsementsCount": proposal.get("endorsementsCount"),
                    "totalCommentsCount": proposal.get("totalCommentsCount"),
                    "versionsCount": proposal.get("versionsCount"),
                    "voteCount": proposal.get("voteCount"),
                    "commentsHaveAlignment": proposal.get("commentsHaveAlignment"),
                    "commentsHaveVotes": proposal.get("commentsHaveVotes"),
                    "createdInMeeting": proposal.get("createdInMeeting"),
                    "hasComments": proposal.get("hasComments"),
                    "official": proposal.get("official"),
                    "fingerprint": proposal.get("fingerprint", {}).get("value"),
                    "position": proposal.get("position"),
                    "reference": proposal.get("reference"),
                    "scope": proposal.get("scope"),
                    "state": proposal.get("state"),
                }
                flattened_data.append(proposal_data)
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


def _get_minio():
    """
    Retorna uma instância do cliente Minio.

    configurado com as informações de conexão fornecidas pelo hook Minio.

    Returns:
    -------
      Minio: Uma instância do cliente Minio
      configurado com as informações de conexão fornecidas pelo hook Minio.
    """
    minio_conn = BaseHook.get_connection(MINIO_CONN_ID)
    minio_host = minio_conn.host.split("//")[1]
    minio_access_key = minio_conn.login
    minio_secret_access = minio_conn.password
    client = Minio(
        minio_host,
        access_key=minio_access_key,
        secret_key=minio_secret_access,
        secure=False,
    )
    return client


@dag(
    default_args={
        "owner": "Amoêdo/Nitai",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    },
    schedule="0 23 * * *",
    catchup=False,
    start_date=datetime(2023, 11, 10),
    description=__doc__,
    tags=["decidim", "minio"],
    dag_id="fetch_process_and_clean_proposals",
)
def fetch_process_and_clean_proposals():
    """DAG que extrai dados de propostas de um GraphQL API e os armazena em um bucket MinIO."""

    @task(provide_context=True)
    def get_data(**context):
        """
        Fetches data from a GraphQL API and stores it in a MinIO bucket.

        Args:
        ----
          **context: The context dictionary containing the execution date.

        Returns:
        -------
          None
        """
        date = context["execution_date"].strftime("%Y-%m-%d")
        next_date = (context["execution_date"] + timedelta(days=1)).strftime("%Y-%m-%d")
        date_file = context["execution_date"].strftime("%Y%m%d")
        # Fetch data from GraphQL API
        hook = GraphQLHook(DECIDIM_CONN_ID)
        session = hook.get_session()
        response = session.post(
            hook.api_url,
            json={"query": QUERY, "variables": {"start_date": f"{date}", "end_date": f"{next_date}"}},
        )
        dado = response.json()
        # Store data in MinIO bucket
        minio = _get_minio()
        minio.put_object(
            MINIO_BUCKET,
            LANDING_ZONE_FILE_NAME.format(date_file=date_file),
            io.BytesIO(json.dumps(dado).encode()),
            len(json.dumps(dado).encode()),
        )

    @task(provide_context=True, retries=3, retry_delay=timedelta(seconds=5))
    def salva_minio(**context):
        date_file = context["execution_date"].strftime("%Y%m%d")
        minio = _get_minio()
        dado = minio.get_object(MINIO_BUCKET, LANDING_ZONE_FILE_NAME.format(date_file=date_file))
        dado = json.loads(dado.read())
        dado = flatten_structure_with_additional_fields(dado)
        df = pd.DataFrame(dado)
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        minio.put_object(
            MINIO_BUCKET,
            PROCESSED_FILE_NAME.format(date_file=date_file),
            io.BytesIO(csv_buffer.getvalue().encode()),
            len(csv_buffer.getvalue().encode()),
        )

    @task(provide_context=True)
    def delete_landing_zone_file(**context):
        date_file = context["execution_date"].strftime("%Y%m%d")
        minio = _get_minio()
        minio.remove_object(MINIO_BUCKET, LANDING_ZONE_FILE_NAME.format(date_file=date_file))

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    start >> get_data() >> salva_minio() >> delete_landing_zone_file() >> end


fetch_process_and_clean_proposals()
