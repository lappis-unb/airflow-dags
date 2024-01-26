from airflow.decorators import dag, task
from plugins.graphql.hooks.graphql import GraphQLHook
from airflow.hooks.base import BaseHook
from pathlib import Path
from datetime import datetime, timedelta
import requests
import inflect
from inflection import underscore


BP_CONN_ID = "Brasil_Participativo"

MATOMO_ENPOINTS = [
        ('VisitsSummary', 'get'),  
        ('VisitFrequency', 'get'), 
        ('UserCountry', 'getRegion'), 
        ('DevicesDetection', 'getType')
]


def _get_components_id_from_participatory_space(participatory_space_id:int, participatory_space_type:str):

    accepted_components_types = ["Proposasls"]
    graph_ql_hook = GraphQLHook(BP_CONN_ID)
    query = Path(__file__).parent.joinpath(f"./queries/participatory_spaces/{participatory_space_type}.gql").open().read()
    query_result = graph_ql_hook.run_graphql_query(query, variables={"space_id":  participatory_space_id})

    query_data = query_result["data"][list(query_result["data"].keys())[0]]

    components_inside_participatory_space = query_data.get("components", None)
    inflect_engine = inflect.engine()
    link_participatory_space_type = underscore(query_data['__typename']).split("_")[-1]
    participatory_space_url = f"{graph_ql_hook.api_url}/{inflect_engine.plural(link_participatory_space_type)}/{query_data['slug']}"


    accepted_components = []
    for component in components_inside_participatory_space:
        if component["__typename"] in accepted_components_types:
            accepted_components.append(component)

    return {
            "accepted_components": accepted_components,
            "participatory_space_url": participatory_space_url
    }

def _get_proposals_data(component_id: int, start_date: str, end_date: str):
    query = Path(__file__).parent.joinpath(f"./queries/components/get_proposals_by_component_id.gql").open().read()
    query_result = GraphQLHook(DECIDIM_CONN_ID).run_graphql_paginated_query(query, variables={"id": component_id, "start_date": start_date, "end_date": end_date})

    result_proposals_data = []
    result_partipatory_space_link = []

    for page in query_result:

        component = page["data"]["component"]
        page_component_id = component["id"]
        partipatory_space_id = component["participatorySpace"]["id"]
        partipatory_space_type = component["participatorySpace"]["type"].split("::")[-1]
        page_component_name = component["name"].get("translation", "-")
        page_proposals = component["proposals"]["nodes"]

        for proposal in page_proposals:
            result_proposals_data.append({
                "page_component_id": page_component_id,
                "partipatory_space_id": partipatory_space_id,
                "partipatory_space_type": partipatory_space_type,
                "page_component_name": page_component_name,
                "proposal_id": proposal["id"],
                "proposal_title": proposal["title"].get("translation", "-"),
                "proposal_published_at": proposal["publishedAt"],
                "proposal_updated_at": proposal["updatedAt"],
                "proposal_state":  proposal["state"],
                "proposal_total_comments": proposal["totalCommentsCount"],
                "proposal_total_votes": proposal["voteCount"],
                "proposal_category_title": proposal["category"]["name"].get("translation", "-") if proposal["category"] else "-",
            })
    return result_proposals_data

def _get_matomo_data(url: list, start_date: str, end_date: str, module: str, method: str):
    matomo_connection = BaseHook.get_connection('matomo_conn')
    MATOMO_URL = matomo_connection.host
    TOKEN_AUTH = matomo_connection.password
    SITE_ID = matomo_connection.login
    date_filter = f"{start_date},{end_date}"

    params = {
        'module': 'API',
        'idSite': SITE_ID,
        'period': 'range',
        'date': date_filter,
        'segment': f'pageUrl={url}',
        'format': 'json',
        'token_auth': TOKEN_AUTH,
        'method': f'{module}.{method}'
    }
    response = requests.get(MATOMO_URL, params=params)


@dag(
    default_args={
    "owner": "Joyce/Paulo",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
},

    schedule=None,
    catchup=False,
    start_date=datetime(2023, 11, 10),
    description=__doc__,
    tags=["decidim", "reports"],
    )

def generate_report_bp(email: str, start_date: str, end_date:str, participatory_space_id:int, participatory_space_type:str):
    """
        1. Pegar todos os componentes do espaço participativo.
            1. Fazer a query para o BP com as datas passadas.
        2. Fazer a requisição para o matomo com as datas passadas.
        3. Gerar o relatorio.
    """

    @task
    def get_components_id(space_id:int, space_type: str):
        return _get_components_id_from_participatory_space(space_id, space_type)

    @task
    def get_components_data(components_data:list, filter_start_date: str, filter_end_date: str):

        result = []
        for component_data in components_data:
            if component_data["__typename"] == "Proposals":
                result.extend(_get_proposals_data(component_data["id"], filter_start_date, filter_end_date))
        
        return result
    
    get_components_data(get_components_id(space_id=participatory_space_id, space_type=participatory_space_type), filter_start_date=start_date, filter_end_date=end_date)

generate_report_bp("test@gmail.com", "2023-01-01", "2024-01-01", 4, "participatory_process")