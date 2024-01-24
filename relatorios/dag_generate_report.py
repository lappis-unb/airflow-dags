from airflow.decorators import dag, task
from plugins.graphql.hooks.graphql import GraphQLHook
from pathlib import Path

DECIDIM_CONN_ID = "api_decidim"

def _get_components_id_from_participatory_space(participatory_space_id:int, participatory_space_type:str):

    accepted_components_types = ["Proposasls"]

    query = Path(__file__).parent.joinpath(f"./queries/participatory_spaces/{participatory_space_type}.gql").open().read()
    query_result = GraphQLHook(DECIDIM_CONN_ID).run_graphql_query(query, variables={"space_id":  participatory_space_id})
    components_inside_participatory_space = query_result["data"][list(query_result["data"].keys())[0]].get("components", None)
    
    accepted_components = []
    for component in components_inside_participatory_space:
        if component["__typename"] in accepted_components_types:
            accepted_components.append(component)

    return accepted_components


def _get_proposals_data(component_id: int, start_date: str, end_date: str):
    query = Path(__file__).parent.joinpath(f"./queries/components/get_proposals_by_component_id.gql").open().read()

    query_result = GraphQLHook(DECIDIM_CONN_ID).run_graphql_paginated_query(query, variables={"id": component_id, "start_date": start_date, "end_date": end_date})

    result_proposals_data = []
    for page in query_result:

        component = page["data"]["component"]
        page_component_id = component["id"]
        partipatory_space_id = component["participatorySpace"]["id"]
        partipatory_space_type = component["participatorySpace"]["type"].split("::")[-1]
        page_component_name = component["name"].get("translation", "-")

        page_proposals = component["proposals"]["nodes"]
        for proposal in page_proposals:
            proposal_id = proposal["id"]
            proposal_title = proposal["title"].get("translation", "-")
            proposal_published_at = proposal["publishedAt"]
            proposal_updated_at = proposal["updatedAt"]
            proposal_state = proposal["state"]
            proposal_total_comments = proposal["totalCommentsCount"]
            proposal_total_votes = proposal["voteCount"]
            proposal_category_title = proposal["category"]["name"].get("translation", "-") if proposal["category"] else "-"

            result_proposals_data.append({
                "page_component_id": page_component_id,
                "partipatory_space_id": partipatory_space_id,
                "partipatory_space_type": partipatory_space_type,
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


@dag(

    )
def generate_report_bp(email: str, start_date: str, end_date:str, participatory_space_id:int, participatory_space_type:str):
    """
        1. Pegar todos os componentes do espaço participativo.
            1. Fazer a query para o BP com as datas passadas.
        2. Fazer a requisição para o matomo com as datas passadas.
        3. Gerar o relatorio.
    """

    @task
    def get_components_ids(space_id:int, space_type: str):
        return _get_components_id_from_participatory_space(space_id, space_type)

    @task
    def get_components_data(components_data:list, filter_start_date: str, filter_end_date: str):

        result = []
        for component_data in components_data:
            if component_data["__typename"] == "Proposals":
                result.extend(_get_proposals_data(component_data["id"], filter_start_date, filter_end_date))
        
        return result