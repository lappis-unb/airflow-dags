import asyncio
import logging
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.telegram.hooks.telegram import TelegramHook
from unidecode import unidecode

from plugins.graphql.hooks.graphql_hook import GraphQLHook
from plugins.telegram.decorators import telegram_retry
from plugins.yaml.config_reader import dump_yaml, load_yaml, read_yaml_files_from_directory

DECIDIM_CONN_ID = "api_decidim"
TELEGRAM_CONN_ID = "telegram_decidim"
VARIABLE_FOR_LAST_DATE_EXECUTED = "last_config_creation_date"
CONFIG_FOLDER = Path(
    os.environ.get("AIRFLOW_HOME", "/opt/airflow/") / Path("dags-data/Notifications-Configs")
)
ACCEPTED_COMPONENTS_TYPES = ["Proposals"]
TELEGRAM_MAX_RETRIES = 10

PROPOSALS_TOPICS_TO_CREATE = {
    "telegram_moderation_proposals_topic_id": lambda name: f"{name}/Propostas",
    "telegram_moderation_comments_topic_id": lambda name: f"{name}/Comentarios Em Propostas",
}


def _get_participatory_space_mapped_to_query_file(participatory_spaces: List[str]):
    queries_folder = Path(__file__).parent.joinpath("./queries")
    queries_files = {
        participatory_space: queries_folder.joinpath(f"./components_in_{participatory_space}.gql")
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

    new_telegram_topic = asyncio.run(telegram_hook.get_conn().create_forum_topic(chat_id=chat_id, name=name))
    logging.info(type(new_telegram_topic))

    return new_telegram_topic.message_thread_id


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
    try:
        participatory_space_chat_id = (
            int(participatory_space["groupChatId"]) if participatory_space["groupChatId"] else None
        )
    except ValueError:
        participatory_space_chat_id = None

    for component in participatory_space["components"]:
        if component["__typename"] in accepeted_component_types:
            component_name = component["name"].get("translation", "").upper().replace(" ", "_")
            participatory_space_slug = participatory_space_slug.upper().replace(" ", "_")
            configure_infos = {
                "__typename": component["__typename"],
                "process_id": re.sub(
                    r"[^\w\d]",
                    "_",
                    unidecode(
                        f"{participatory_space_slug}_{component_name}", errors="replace", replace_str="_"
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
    logging.info(CONFIG_FOLDER)
    if not CONFIG_FOLDER.exists():
        CONFIG_FOLDER.mkdir(parents=True, exist_ok=True)

    configured_processes = {x["component_id"]: x for x in read_yaml_files_from_directory(CONFIG_FOLDER)}

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


def _configure_telegram_topic(config_name: str, topic_naming_func: Any, component_config):
    """Configure a Telegram topic based on the provided configuration.

    Args:
    ----
        config_name (str): The name of the configuration.
        topic_naming_func (Any): The function used to generate topic names.
        component_config: Configuration details for the component.

    Returns:
    -------
        dict: A dictionary containing the configured Telegram topic.

    """
    telegram_topics = {}
    name = " ".join(str(component_config["process_id"]).split("_")).title().strip()

    telegram_topics = {
        config_name: _create_telegram_topic(
            component_config["telegram_config"]["telegram_group_id"], topic_naming_func(name)
        )
    }
    return telegram_topics


def _get_telegram_topics(component: dict, old_config: Optional[dict] = None):
    telegram_topics_keys_configured = set(component["telegram_config"].keys())
    if old_config:
        telegram_topics_keys_configured = set(old_config["telegram_config"].keys())
    logging.info("Telgram keys already configured: %s", telegram_topics_keys_configured)

    telegram_topics_to_create = set(PROPOSALS_TOPICS_TO_CREATE.keys()).difference(
        telegram_topics_keys_configured
    )
    logging.info("Telgram keys to configure: %s", telegram_topics_to_create)

    return telegram_topics_to_create


def _update_telegram_config(component: dict, old_config: Optional[dict] = None):
    assert isinstance(component, dict)

    if not component["telegram_config"]["telegram_group_id"]:
        return component["telegram_config"]

    telegram_topics_to_create = _get_telegram_topics(component, old_config)

    if len(telegram_topics_to_create) == 0:
        return {**component["telegram_config"], **old_config["telegram_config"]}

    telegram_topics = {}
    for topic in telegram_topics_to_create:
        telegram_topics = {
            **telegram_topics,
            **_configure_telegram_topic(
                config_name=topic,
                topic_naming_func=PROPOSALS_TOPICS_TO_CREATE.get(topic),
                component_config=component,
            ),
        }

    if old_config:
        return {
            **component["telegram_config"],
            **(old_config["telegram_config"]),
            "telegram_group_id": component["telegram_group_id"],
            **telegram_topics,
        }
    return {
        **component["telegram_config"],
        "telegram_group_id": component["telegram_group_id"],
        **telegram_topics,
    }


def _update_old_config(new_config: dict, old_config: dict):
    """Update old configuration with new configuration details.

    Args:
    ----
        new_config (dict): The new configuration to update from.
        old_config (dict): The old configuration to be updated.

    Returns:
    -------
        dict: The updated old configuration.
    """
    assert isinstance(new_config, dict)
    assert isinstance(old_config, dict)

    if "__typename" in new_config:
        new_config.pop("__typename")
    if "__typename" in old_config:
        old_config.pop("__typename")

    old_config["start_date"] = _str_to_datetime(new_config["start_date"])
    old_config["end_date"] = _str_to_datetime(new_config["end_date"]) if new_config["end_date"] else None

    return old_config


DEFAULT_ARGS = {
    "owner": "Paulo G.",
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
        return datetime.strptime(update_datetime, date_format) if update_datetime is not None else None

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
                to_configure, to_update = _split_components_between_configure_and_update(participatory_space)
                components_to_configure.extend(to_configure)
                components_to_update.extend(to_update)

        return {
            "components_to_configure": components_to_configure,
            "components_to_update": components_to_update,
        }

    @task
    def configure_component(components_to_configure):
        """Configure components based on the provided configurations.

        This function iterates through a list of component configurations, creates folders
        for each component type, updates the Telegram configuration, processes the component
        by updating its old configuration with new details, and saves the processed component
        as a YAML file.

        Args:
        ----
            components_to_configure (list): A list of dictionaries representing component configurations.

        """
        logging.info("Configuring dags in folder %s.", CONFIG_FOLDER)
        for component in components_to_configure:
            component_type_folder = CONFIG_FOLDER.joinpath(f"./{component['__typename']}")
            component_type_folder.mkdir(parents=True, exist_ok=True)

            component["telegram_config"] = _update_telegram_config(component)
            processed_component = _update_old_config(component, component)

            yaml_file_path = component_type_folder.joinpath(f"./{component['component_id']}.yaml")

            logging.info("Yaml configurado \n %s .", processed_component)

            dump_yaml(processed_component, yaml_file_path)

    @task
    def update_component(components_to_update):
        """Update components based on the provided configurations.

            This function takes a list of dictionaries representing component configurations
        to be updated. For each component, it loads the existing configuration from a YAML
        file, incorporates any new configuration details provided, updates the Telegram
        configuration, and saves the updated configuration back to the YAML file.

            Args:
            ----
                components_to_update (list): A list of dictionaries representing component configurations.

        """
        logging.info("Updating dags in folder %s.", CONFIG_FOLDER)
        for component in components_to_update:
            component_type_folder = CONFIG_FOLDER.joinpath(f"./{component['__typename']}")
            component_type_folder.mkdir(parents=True, exist_ok=True)

            yaml_file_path = component_type_folder.joinpath(f"./{component['component_id']}.yaml")
            old_config = load_yaml(yaml_file_path)

            old_config["telegram_config"] = _update_telegram_config(component, old_config)
            old_config = _update_old_config(component, old_config)

            logging.info("Yaml configurado \n %s .", old_config)

            dump_yaml(old_config, yaml_file_path)

    filter_and_configure_componets_task = filter_and_configure_componets(*tasks_to_get_all_components)
    configure_component(filter_and_configure_componets_task["components_to_configure"])
    update_component(filter_and_configure_componets_task["components_to_update"])


create_processes_configs()
