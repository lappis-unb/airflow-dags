import logging
from datetime import datetime, timedelta
from itertools import chain
from pathlib import Path

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
    "owner": "Paulo/Isaque",
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


def _filter_out_components(segmented_ids: set, *set_of_participatory_spaces):
    set_all_components = set(chain.from_iterable(set_of_participatory_spaces))
    return set_all_components.difference(segmented_ids)


