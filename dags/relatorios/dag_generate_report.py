from airflow.decorators import dag, task
from plugins.graphql.hooks.graphql_hook import GraphQLHook
from plugins.reports.report import ReportGenerator
from airflow.hooks.base import BaseHook
from pathlib import Path
from datetime import datetime, timedelta
import requests
import inflect
from inflection import underscore
import logging

BP_CONN_ID = "bp_conn"

MATOMO_ENPOINTS = [
        ('VisitsSummary', 'get'),  
        ('VisitFrequency', 'get'), 
        ('UserCountry', 'getRegion'), 
        ('DevicesDetection', 'getType')
]

def _get_components_id_from_participatory_space(participatory_space_id:int, participatory_space_type:str):

    accepted_components_types = ["Proposals"]
    participatory_space_url = "https://brasilparticipativo.presidencia.gov.br"

    graph_ql_hook = GraphQLHook(BP_CONN_ID)
    query = Path(__file__).parent.joinpath(f"./queries/participatory_spaces/{participatory_space_type}.gql").open().read()
    query_result = graph_ql_hook.run_graphql_query(query, variables={"space_id": participatory_space_id})

    participatory_space_data = query_result["data"][list(query_result["data"].keys())[0]]
    inflect_engine = inflect.engine()

    link_participatory_space_type = underscore(participatory_space_data['__typename']).split("_")[-1]
    participatory_space_url = f"{participatory_space_url}/{inflect_engine.plural(link_participatory_space_type)}/{participatory_space_data['slug']}"

    accepted_components = []
    for component in participatory_space_data["components"]:
        if component["__typename"] in accepted_components_types:
            accepted_components.append(component)

    return {
        "accepted_components": accepted_components,
        "participatory_space_url": participatory_space_url
    }

def _get_proposals_data(component_id: int, start_date: str, end_date: str):
    query = Path(__file__).parent.joinpath("./queries/components/get_proposals_by_component_id.gql").open().read()
    query_result = GraphQLHook(BP_CONN_ID).run_graphql_paginated_query(query, variables={"id": component_id, "start_date": start_date, "end_date": end_date})

    result_proposals_data = []
    for page in query_result:
        component = page.get("data", {}).get("component", {})
        if not component:
            continue
        
        page_component_id = component.get("id")
        participatory_space_id = component.get("participatorySpace", {}).get("id")
        participatory_space_type = component.get("participatorySpace", {}).get("type", "").split("::")[-1]
        page_component_name = component.get("name", {}).get("translation", "-")
        page_proposals = component.get("proposals", {}).get("nodes", [])

        for proposal in page_proposals:
            proposal_id = proposal.get("id")
            proposal_title = proposal.get("title", {}).get("translation", "-")
            proposal_published_at = proposal.get("publishedAt")
            proposal_updated_at = proposal.get("updatedAt")
            proposal_state = proposal.get("state")
            proposal_total_comments = proposal.get("totalCommentsCount")
            proposal_total_votes = proposal.get("voteCount")
            proposal_category_title = proposal.get("category", {}).get("name", {}).get("translation", "-") if proposal.get("category") else "-"

            result_proposals_data.append({
                "page_component_id": page_component_id,
                "participatory_space_id": participatory_space_id,
                "participatory_space_type": participatory_space_type,
                "page_component_name": page_component_name,
                "proposal_id": proposal_id,
                "proposal_title": proposal_title,
                "proposal_published_at": proposal_published_at,
                "proposal_updated_at": proposal_updated_at,
                "proposal_state": proposal_state,
                "proposal_total_comments": proposal_total_comments,
                "proposal_total_votes": proposal_total_votes,
                "proposal_category_title": proposal_category_title,
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
        'segment': f'pageUrl=^{url}',
        'format': 'csv',
        'token_auth': TOKEN_AUTH,
        'method': f'{module}.{method}'
    }
    logging.info("Params para a requisição do matomo \n%s.", params)

    response = requests.get(MATOMO_URL, params=params)
    response.raise_for_status()

    try:
        return response.text
    except requests.exceptions.JSONDecodeError as error:
        logging.exception("Response text: %s", response.text)
        raise error

def _generate_report(bp_data, *matomo_data):
    print(bp_data)
    print(*matomo_data)
    print(type(*matomo_data))
    
    report_generator = ReportGenerator()
    df_bp = report_generator.create_bp_dataframe(bp_data)

    num_proposals, num_votes, num_comments = report_generator.calculate_totals(df_bp)
    data_to_insert = {
        "Propostas": num_proposals,
        "Votos": num_votes,
        "Comentários": num_comments
    }
    
    daily_graph = report_generator.generate_daily_plot(df_bp)
    
    device_graph = report_generator.generate_device_graph(*matomo_data)
    rank_temas = report_generator.generate_theme_ranking(df_bp)
    top_proposals_filtered = report_generator.generate_top_proposals(df_bp)
    
    shp_path = Path(__file__).parent.joinpath("./shapefile/estados_2010.shp").resolve()
    brasil, dados = report_generator.load_data(shp_path, matomo_data[0]) 
    dados_brasil = report_generator.filter_and_rename(dados, 'br', 'UF')
    mapa = report_generator.create_map(brasil, dados_brasil, 'sigla', 'UF')
    
    map_graph = report_generator.plot_map(mapa, 'nb_visits')
    
    final_html = report_generator.generate_html_report(rank_temas, top_proposals_filtered, daily_graph, device_graph, map_graph, template_dir="caminho_para_seu_template")

    return final_html

        
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
    tags=["decidim", "reports", "participacao", "bp"],
    )

def generate_report_bp(email: str, start_date: str, end_date:str, participatory_space_id:int, participatory_space_type:str):
    """
        1. Pegar todos os componentes do espaço participativo.
            1. Fazer a query para o BP com as datas passadas.
        2. Fazer a requisição para o matomo com as datas passadas.
        3. Gerar o relatorio.
    """

    @task(multiple_outputs=True)
    def get_components_id(space_id:int, space_type: str):
        return _get_components_id_from_participatory_space(space_id, space_type)

    @task
    def get_components_data(components_data:list, filter_start_date: str, filter_end_date: str):

        result = []
        for component_data in components_data:
            if component_data["__typename"] == "Proposals":
                result.extend(_get_proposals_data(component_data["id"], filter_start_date, filter_end_date))
        
        return result
    
    get_components_id_task = get_components_id(space_id=participatory_space_id, space_type=participatory_space_type)

    matomo_tasks = []
    for module_ep, method_ep in MATOMO_ENPOINTS:
        @task(
            task_id=f"get_matomo_{module_ep}_{method_ep}"
        )
        def generator_matomo_extractor(url: list, filter_start_date: str, filter_end_date: str, module: str, method: str):
            return _get_matomo_data(url=url, start_date=filter_start_date, end_date=filter_end_date, module=module, method=method)
        
        matomo_tasks.append(generator_matomo_extractor(get_components_id_task["participatory_space_url"], start_date, end_date, module_ep, method_ep))

    @task
    def generate_report(bp_data, *matomo_data):
        return _generate_report(bp_data, *matomo_data)

    get_components_data_task = get_components_data(get_components_id_task["accepted_components"], filter_start_date=start_date, filter_end_date=end_date)

    generate_report(get_components_data_task, *matomo_tasks)

generate_report_bp("test@gmail.com", "2023-01-01", "2024-01-01", 2, "participatory_process")


