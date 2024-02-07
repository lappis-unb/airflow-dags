import logging
from datetime import datetime, timedelta
from pathlib import Path

import inflect
import requests
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from inflection import underscore

from plugins.graphql.hooks.graphql_hook import GraphQLHook
from plugins.reports.report import ReportGenerator

BP_CONN_ID = "bp_conn"


def _get_components_id_from_participatory_space(participatory_space_id: int, participatory_space_type: str):
    accepted_components_types = ["Proposals"]
    space_url = "https://brasilparticipativo.presidencia.gov.br"

    graph_ql_hook = GraphQLHook(BP_CONN_ID)
    query = (
        Path(__file__)
        .parent.joinpath(f"./queries/participatory_spaces/{participatory_space_type}.gql")
        .open()
        .read()
    )
    query_result = graph_ql_hook.run_graphql_query(query, variables={"space_id": participatory_space_id})

    participatory_space_data = query_result["data"][next(iter(query_result["data"].keys()))]
    logging.info(participatory_space_data)
    inflect_engine = inflect.engine()
    link_space_type = inflect_engine.plural(underscore(participatory_space_data["__typename"]).split("_")[-1])
    participatory_space_url = f"{space_url}/{link_space_type}/{participatory_space_data['slug']}"

    accepted_components = []
    for component in participatory_space_data["components"]:
        if component["__typename"] in accepted_components_types:
            accepted_components.append(component)

    return {"accepted_components": accepted_components, "participatory_space_url": participatory_space_url}


def _get_proposals_data(component_id: int, start_date: str, end_date: str):
    query = (
        Path(__file__).parent.joinpath("./queries/components/get_proposals_by_component_id.gql").open().read()
    )
    query_result = GraphQLHook(BP_CONN_ID).run_graphql_paginated_query(
        query, variables={"id": component_id, "start_date": start_date, "end_date": end_date}
    )

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
            proposal_category_title = (
                proposal.get("category", {}).get("name", {}).get("translation", "-")
                if proposal.get("category")
                else "-"
            )

            result_proposals_data.append(
                {
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
                }
            )
    return result_proposals_data


def _get_matomo_data(url: list, start_date: str, end_date: str, module: str, method: str):
    matomo_connection = BaseHook.get_connection("matomo_conn")
    matomo_url = matomo_connection.host
    token_auth = matomo_connection.password
    site_id = matomo_connection.login
    date_filter = f"{start_date},{end_date}"
    params = {
        "module": "API",
        "idSite": site_id,
        "period": "range",
        "date": date_filter,
        "segment": f"pageUrl=^{url}",
        "format": "csv",
        "token_auth": token_auth,
        "method": f"{module}.{method}",
    }
    logging.info("Params para a requisição do matomo \n%s.", params)

    response = requests.get(matomo_url, params=params)
    response.raise_for_status()

    try:
        return response.text
    except requests.exceptions.JSONDecodeError as error:
        logging.exception("Response text: %s", response.text)
        raise error


def _generate_graphs(bp_data, visits_summary, visits_frequency, user_contry, devices_detection):
    report_generator = ReportGenerator()
    df_bp = report_generator.create_bp_dataframe(bp_data)

    num_proposals, num_votes, num_comments = report_generator.calculate_totals(df_bp)
    # data_to_insert = {"Propostas": num_proposals, "Votos": num_votes, "Comentários": num_comments}

    daily_graph = report_generator.generate_daily_plot(df_bp)

    device_graph = report_generator.generate_device_graph(devices_detection)
    rank_temas = report_generator.generate_theme_ranking(df_bp)
    top_proposals_filtered = report_generator.generate_top_proposals(df_bp)

    shp_path = Path(__file__).parent.joinpath("./shapefile/estados_2010.shp").resolve()
    brasil, dados = report_generator.load_data(shp_path, user_contry)
    dados_brasil = report_generator.filter_and_rename(dados, "br", "UF")
    mapa = report_generator.create_map(brasil, dados_brasil, "sigla", "UF")

    map_graph = report_generator.plot_map(mapa, "nb_visits")

    # final_html = report_generator.generate_html_report(
    #     rank_temas,
    #     top_proposals_filtered,
    #     daily_graph,
    #     device_graph,
    #     map_graph,
    #     template_dir="caminho_para_seu_template",
    # )

    return {
        "rank_temas": rank_temas,
        "top_proposals": top_proposals_filtered,
        "daily_graph": daily_graph,
        "device_graph": device_graph,
        "map_graph": map_graph,
    }


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
def generate_report_bp(
    email: str, start_date: str, end_date: str, participatory_space_id: int, participatory_space_type: str
):
    """
    Gera um relatorio para o BP.

    1. Pegar todos os componentes do espaço participativo.
        1. Fazer a query para o BP com as datas passadas.
    2. Fazer a requisição para o matomo com as datas passadas.
    3. Gerar o relatorio.
    """

    @task(multiple_outputs=True)
    def get_components_id(space_id: int, space_type: str):
        return _get_components_id_from_participatory_space(space_id, space_type)

    @task
    def get_components_data(components_data: list, filter_start_date: str, filter_end_date: str):
        result = []
        for component_data in components_data:
            if component_data["__typename"] == "Proposals":
                result.extend(_get_proposals_data(component_data["id"], filter_start_date, filter_end_date))

        return result

    get_components_id_task = get_components_id(
        space_id=participatory_space_id, space_type=participatory_space_type
    )

    def _get_matomo_extractor(matomo_module: str, matomo_method: str):
        @task(task_id=f"get_matomo_{matomo_module}_{matomo_method}")
        def matomo_extractor(
            url: list, filter_start_date: str, filter_end_date: str, module: str, method: str
        ):
            return _get_matomo_data(
                url=url, start_date=filter_start_date, end_date=filter_end_date, module=module, method=method
            )

        return matomo_extractor(
            get_components_id_task["participatory_space_url"],
            start_date,
            end_date,
            matomo_module,
            matomo_method,
        )

    matomo_visits_summary_task = _get_matomo_extractor("VisitsSummary", "get")
    matomo_visits_frequency_task = _get_matomo_extractor("VisitFrequency", "get")
    matomo_user_contry_task = _get_matomo_extractor("UserCountry", "getRegion")
    matomo_devices_detection_task = _get_matomo_extractor("DevicesDetection", "getType")

    @task(multiple_outputs=True)
    def generate_report(bp_data, visits_summary, visits_frequency, user_contry, devices_detection):
        return _generate_graphs(bp_data, visits_summary, visits_frequency, user_contry, devices_detection)

    get_components_data_task = get_components_data(
        get_components_id_task["accepted_components"], filter_start_date=start_date, filter_end_date=end_date
    )

    generate_report(
        get_components_data_task,
        visits_summary=matomo_visits_summary_task,
        visits_frequency=matomo_visits_frequency_task,
        user_contry=matomo_user_contry_task,
        devices_detection=matomo_devices_detection_task,
    )


generate_report_bp("test@gmail.com", "2023-01-01", "2024-01-01", 2, "participatory_process")
