import logging
from datetime import datetime, timedelta
from io import StringIO
from itertools import chain
from pathlib import Path

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable

from plugins.components.base_component.component import ComponentBaseHook
from plugins.graphql.hooks.graphql_hook import GraphQLHook

PARTICIPATORY_SPACES = [
    "participatory_processes",
    "initiatives",
    "consultations",
    "conferences",
    "assemblies",
]
DECIDIM_CONN_ID = "bp_conn_prod"
MATOMO_CONN_ID = "matomo_conn"
SEGMENTED_IDS_VAR = "matomo_segmentation_ids"
ACCEPTED_TYPES = ["Proposals", "Meetings", "Surveys"]


def _get_participatory_space_mapped_to_query_file(participatory_spaces: list[str]):
    queries_folder = Path(__file__).parent.joinpath("./queries/matomo_segmentation")
    queries_files = {
        participatory_space: queries_folder.joinpath(f"./components_in_{participatory_space}.gql")
        for participatory_space in participatory_spaces
    }
    for query in queries_files.values():
        assert query.exists()
    return queries_files


QUERIES = _get_participatory_space_mapped_to_query_file(PARTICIPATORY_SPACES)


DEFAULT_ARGS = {
    "owner": "Paulo G./Isaque",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


def _get_components(query_path):
    hook = GraphQLHook(DECIDIM_CONN_ID)

    query_result: dict[str] = hook.run_graphql_query(hook.get_graphql_query_from_file(query_path))["data"]

    # Todas as queries feitas devem ter apenas uma chave.
    assert len(query_result.keys()) == 1
    mid_key = next(iter(query_result.keys()))

    result = []
    for space in query_result[mid_key]:
        for component in space["components"]:
            if component["__typename"] in ACCEPTED_TYPES:
                result.append(component["id"])
            else:
                logging.info("Component id not accepted: %s", component["id"])
    return result


def _filter_out_components_urls(*participatory_spaces):
    matomo_connection = BaseHook.get_connection(MATOMO_CONN_ID)
    matomo_url = matomo_connection.host
    token_auth = matomo_connection.password
    site_id = matomo_connection.login

    params = {
        "module": "API",
        "method": "SegmentEditor.getAll",
        "idSite": site_id,
        "token_auth": token_auth,
        "format": "csv",
    }
    logging.info("Params para a requisição do matomo \n%s.", params)
    response = requests.post(matomo_url, params=params)
    response.raise_for_status()
    matomo_segmentations = pd.read_csv(StringIO(response.text))

    matomo_segmentations["bp_component_id"] = matomo_segmentations["definition"].apply(
        lambda segmentation: (segmentation.split("/")[-2] if len(segmentation.split("/")) > 1 else None)
    )

    return set(chain.from_iterable(participatory_spaces)).difference(
        set(matomo_segmentations["bp_component_id"])
    )


def _create_matomo_segmentation(segmentation: str):
    matomo_connection = BaseHook.get_connection(MATOMO_CONN_ID)
    matomo_url = matomo_connection.host
    token_auth = matomo_connection.password
    site_id = matomo_connection.login
    splited_segmentation = segmentation.split("/")

    #! TODO: Tirar esse hardcoded quando tivermos um matomo de homolog, quando isso ? nunca saberemos :(.
    assert segmentation.startswith("https://brasilparticipativo.presidencia.gov.br/")

    params = {
        "module": "API",
        "method": "SegmentEditor.add",
        "idSite": site_id,
        "token_auth": token_auth,
        "autoArchive": 1,
        "format": "csv",
        "name": f"{splited_segmentation[-5]}_{splited_segmentation[-4]}_{splited_segmentation[-2]}",
        "definition": f"pageUrl=^{segmentation}",
    }
    logging.info("Params para a requisição do matomo \n%s.", params)
    response = requests.post(matomo_url, params=params)
    response.raise_for_status()

    try:
        return response.text
    except requests.exceptions.JSONDecodeError as error:
        logging.exception("Response text: %s", response.text)
        raise error


@dag(
    default_args=DEFAULT_ARGS,
    schedule_interval="10 * * * *",  # Toda hora, mas com um offset de 10min
    start_date=datetime(2024, 3, 1),
    catchup=False,
    doc_md=__doc__,
    tags=["creation", "dag", "automation", "segmentation", "matomo"],
)
def matomo_segmentation():
    @task
    def get_segmented_ids():
        segmented_ids = eval(Variable.get(SEGMENTED_IDS_VAR, "{'',}"))
        assert isinstance(segmented_ids, set)
        return segmented_ids

    tasks_to_get_all_components = []
    for query_type, query in QUERIES.items():

        @task(task_id=f"get_componets_in_{query_type}")
        def get_componets(query_to_execute):
            return _get_components(query_to_execute)

        tasks_to_get_all_components.append(get_componets(query))

    @task
    def filter_components(*participatory_spaces):
        return _filter_out_components_urls(*participatory_spaces)

    @task
    def get_components_urls(component_ids: list[str]):
        return [
            ComponentBaseHook(DECIDIM_CONN_ID, int(component_id)).get_component_link()
            for component_id in component_ids
        ]

    @task
    def create_matomo_segmentation(components_urls: list[str]):
        # TODO: Adicionar forma de salvar as segmentações ja feitas.

        return [_create_matomo_segmentation(component_url) for component_url in components_urls]

    @task
    def save_segmented_ids(new_ids_segmented: list):
        set_segmented_ids: set = eval(Variable.get(SEGMENTED_IDS_VAR, "{'',}"))
        set_new_ids = set(new_ids_segmented)
        set_segmented_ids = set_segmented_ids.union(set_new_ids)
        try:
            set_segmented_ids.remove("")
        except KeyError:
            logging.info("Key '' does not exists.")
        Variable.set(SEGMENTED_IDS_VAR, set_segmented_ids)

    filter_components_task = filter_components(*tasks_to_get_all_components)
    get_urls_task = get_components_urls(filter_components_task)

    create_matomo_segmentation(get_urls_task)


matomo_segmentation()
