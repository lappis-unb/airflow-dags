# pylint: disable=import-error, pointless-statement, expression-not-assigned, invalid-name

import asyncio
import logging
import re
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

import yaml
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.telegram.hooks.telegram import TelegramHook
from unidecode import unidecode

from plugins.graphql.hooks.graphql_hook import GraphQLHook
from plugins.telegram.decorators import telegram_retry
from plugins.yaml.config_reader import read_yaml_files_from_directory
from airflow.hooks.base_hook import BaseHook

import boto3
from io import StringIO

DECIDIM_CONN_ID = "api_decidim"
TELEGRAM_CONN_ID = "telegram_decidim"
VARIABLE_FOR_LAST_DATE_EXECUTED = "last_config_creation_date"
ACCEPTED_COMPONENTS_TYPES = ["Proposals"]
TELEGRAM_MAX_RETRIES = 10

TOPICS_TO_CREATE = [
    ("telegram_moderation_proposals_topic_id", lambda name: f"{name}/Propostas"),
    (
        "telegram_moderation_comments_topic_id",
        lambda name: f"{name}/Comentarios Em Propostas",
    ),
]


def _create_s3_client():
    minio_conn = BaseHook.get_connection("minio_connection_id")
    minio_url = minio_conn.host
    minio_access_key = minio_conn.login
    minio_secret = minio_conn.password

    return boto3.client(
        "s3",
        endpoint_url=minio_url,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret,
        region_name="us-east-1",
    )


def _get_config_file(filename):
    # filename = f"{component_id}.yaml"
    s3_client = _create_s3_client()
    minio_conn = BaseHook.get_connection("minio_connection_id")
    obj = s3_client.get_object(Bucket=minio_conn.schema, Key=filename)

    return yaml.safe_load(obj["Body"].read().decode("utf-8"))


def _get_all_config_files():
    s3_client = _create_s3_client()
    for key in s3_client.list_objects(Bucket="proposals-config")["Contents"]:
        yield _get_config_file(key["Key"])


def save_to_minio(data, filename):
    # Fetching MinIO connection details from Airflow connections
    minio_conn = BaseHook.get_connection("minio_connection_id")
    minio_conn = minio_conn.host
    minio_bucket = "proposals-config"
    s3_client = _create_s3_client()
    with closing(StringIO()) as buffer:
        yaml.dump(data, buffer)
        buffer.seek(0)
        # Saving JSON to MinIO bucket using boto3
        s3_client.put_object(
            Body=buffer.getvalue(),
            Bucket=minio_bucket,
            Key=filename,
            ContentType="text/yaml",
        )


def _get_participatory_space_mapped_to_query_file(participatory_spaces: List[str]):
    queries_folder = Path(__file__).parent.joinpath("./queries")
    queries_files = {
        participatory_space: queries_folder.joinpath(
            f"./components_in_{participatory_space}.gql"
        )
        for participatory_space in participatory_spaces
    }
    for query in queries_files.values():
        assert query.exists()
    return queries_files


PARTICIPATORY_SPACES = [
    "participatory_processes",
    # "initiatives",
    # "consultations",
    # "conferences",
    # "assemblies",
]
QUERIES = _get_participatory_space_mapped_to_query_file(PARTICIPATORY_SPACES)


def _search_date_key(participatory_space: Dict[str, Any], date_key_pattern):
    for key in participatory_space:
        if isinstance(key, str) and re.search(date_key_pattern, key, re.IGNORECASE):
            logging.info("Returning key: %s for pattern %s", key, date_key_pattern)
            return key
        else:
            logging.warning("Type %s not supported for the key %s.", type(key), key)
    return None


def _str_to_datetime(date_to_change: str):
    return datetime.strptime(date_to_change, "%Y-%m-%d")


@telegram_retry(max_retries=TELEGRAM_MAX_RETRIES)
def _create_telegram_topic(chat_id: int, name: str):
    if not isinstance(chat_id, int) or not isinstance(name, str):
        logging.error("Chat id: %s\nName: %s", chat_id, name)
        raise TypeError

    telegram_hook = TelegramHook(telegram_conn_id=TELEGRAM_CONN_ID, chat_id=chat_id)

    new_telegram_topic = asyncio.run(
        telegram_hook.get_conn().create_forum_topic(chat_id=chat_id, name=name)
    )
    logging.info(type(new_telegram_topic))

    return new_telegram_topic.message_thread_id


def _configure_telegram_topics(component_config):
    telegram_topics = {}
    if component_config["__typename"] == "Proposals":
        name = " ".join(str(component_config["process_id"]).split("_")).title().strip()
        telegram_topics = {
            topic: _create_telegram_topic(
                component_config["telegram_config"]["telegram_group_id"],
                get_chat_name(name),
            )
            for topic, get_chat_name in TOPICS_TO_CREATE
        }
    return telegram_topics


def _configure_base_yaml_in_participatory_spaces(participatory_space):
    accepeted_component_types = ["Proposals"]
    start_date_re_pattern = r"(start|creation).*Date"
    end_date_re_pattern = r"(end|closing).*Date"

    start_date_key = _search_date_key(participatory_space, start_date_re_pattern)
    end_date_key = _search_date_key(participatory_space, end_date_re_pattern)
    participatory_space_start_date = participatory_space[start_date_key]
    participatory_space_end_date = participatory_space[end_date_key]

    participatory_space_start_date = (
        participatory_space_start_date
        if participatory_space_start_date
        else datetime.now().strftime("%Y-%m-%d")
    )

    participatory_space_slug = participatory_space["slug"]
    participatory_space_chat_id = (
        int(participatory_space["groupChatId"])
        if participatory_space["groupChatId"]
        else None
    )

    for component in participatory_space["components"]:
        if component["__typename"] in accepeted_component_types:
            component_name = (
                component["name"].get("translation", "").upper().replace(" ", "_")
            )
            participatory_space_slug = participatory_space_slug.upper().replace(
                " ", "_"
            )
            configure_infos = {
                "__typename": component["__typename"],
                "process_id": re.sub(
                    r"[^\w\d]",
                    "_",
                    unidecode(
                        f"{participatory_space_slug}_{component_name}",
                        errors="replace",
                        replace_str="_",
                    ),
                ).strip("_"),
                "component_id": int(component["id"]),
                "start_date": participatory_space_start_date,
                "end_date": participatory_space_end_date,
                "decidim_url": "",
                "telegram_config": {
                    "telegram_conn_id": TELEGRAM_CONN_ID,
                    "telegram_group_id": participatory_space_chat_id,
                },
            }

            yield configure_infos


def _split_components_between_configure_and_update(participatory_space):
    components_to_configure = []
    components_to_update = []

    configured_processes = {x["component_id"]: x for x in _get_all_config_files()}

    for config in _configure_base_yaml_in_participatory_spaces(participatory_space):
        if config["__typename"] not in ACCEPTED_COMPONENTS_TYPES:
            continue

        if config["component_id"] in configured_processes:
            components_to_update.append(config)
        else:
            components_to_configure.append(config)

    logging.info("Total components to configure %s", len(components_to_configure))
    logging.info("Total components to update %s", len(components_to_update))

    return components_to_configure, components_to_update


DEFAULT_ARGS = {
    "owner": "Paulo",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    default_args=DEFAULT_ARGS,
    schedule_interval="10 */1 * * *",  # Toda hora, mas com um offset de 10min
    start_date=datetime(2023, 11, 18),
    catchup=False,
    doc_md=__doc__,
    tags=["creation", "dag", "automation"],
)
def create_processes_configs():
    @task
    def get_update_date() -> datetime:
        """Airflow task that retrieve last comment update date from airflow variables.

        Returns
        -------
            datetime: last comment update from airflow variables.
        """
        date_format = "%Y-%m-%d"
        update_datetime = Variable.get(VARIABLE_FOR_LAST_DATE_EXECUTED, None)
        return (
            datetime.strptime(update_datetime, date_format)
            if update_datetime is not None
            else None
        )

    get_update_date_task = get_update_date()

    tasks_to_get_all_components = []
    for query_type, query in QUERIES.items():

        @task(task_id=f"get_componets_in_{query_type}")
        def get_componets(filter_date, query_to_execute):
            hook = GraphQLHook(DECIDIM_CONN_ID)

            variables = None
            if filter_date is not None:
                variables = {"date": filter_date}

            logging.info("Query variables - %s", variables)
            query_result: dict[str] = hook.run_graphql_query(
                hook.get_graphql_query_from_file(query_to_execute), variables=variables
            )["data"]
            # Todas as queries feitas devem ter apenas uma chave.
            assert len(query_result.keys()) == 1
            mid_key = next(iter(query_result.keys()))
            return query_result[mid_key]

        tasks_to_get_all_components.append(get_componets(get_update_date_task, query))

    @task(multiple_outputs=True)
    def filter_and_configure_componets(*set_of_participatory_spaces):
        components_to_configure = []
        components_to_update = []

        for participatory_spaces_in_set in set_of_participatory_spaces:
            for participatory_space in participatory_spaces_in_set:
                logging.info(participatory_space)
                to_configure, to_update = (
                    _split_components_between_configure_and_update(participatory_space)
                )
                components_to_configure.extend(to_configure)
                components_to_update.extend(to_update)

        return {
            "components_to_configure": components_to_configure,
            "components_to_update": components_to_update,
        }

    @task
    def configure_component(componentes_to_configure):
        for _component in componentes_to_configure:
            if _component["telegram_config"]["telegram_group_id"]:
                telegram_topics = _configure_telegram_topics(_component)
                _component["telegram_config"] = {
                    **_component["telegram_config"],
                    **telegram_topics,
                }
            _component.pop("__typename")

            _component["start_date"] = _str_to_datetime(_component["start_date"])
            _component["end_date"] = (
                _str_to_datetime(_component["end_date"])
                if _component["end_date"]
                else None
            )

            save_to_minio(_component, f"{_component['component_id']}.yaml")

    @task
    def update_component(componentes_to_update):
        for _component in componentes_to_update:
            old_config = _get_config_file(f"{_component['component_id']}.yaml")

            if (
                _component["telegram_config"]["telegram_group_id"]
                and not old_config["telegram_config"]["telegram_group_id"]
            ):
                telegram_topics = _configure_telegram_topics(_component)
                old_config["telegram_config"] = {
                    **_component["telegram_config"],
                    **telegram_topics,
                }

            _component.pop("__typename")

            old_config["start_date"] = _str_to_datetime(_component["start_date"])
            old_config["end_date"] = (
                _str_to_datetime(_component["end_date"])
                if _component["end_date"]
                else None
            )

            save_to_minio(old_config, f"{_component['component_id']}.yaml")

    filter_and_configure_componets_task = filter_and_configure_componets(
        *tasks_to_get_all_components
    )
    configure_component(filter_and_configure_componets_task["components_to_configure"])
    update_component(filter_and_configure_componets_task["components_to_update"])


create_processes_configs()
