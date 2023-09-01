"""
DAG to query Decidim software recent proposals and send result to
telegram chat.

The DAG flow is:
1. [task: get_update_date] Get the last telegram message proposal date.
This variable is used to query the API and to filter the results.
The API only accepts filter on format YYYY-MM-DD but the DAG runs on
minutes interval.
2. [task: get_proposals] Use the last_date to query the day's proposals.
3. [task: mount_telegram_messages] Parse the API json response and select
only the proposals that are newer (or updated) than the value get on
step 1. It consider HH:MM not filtered by the API.
4. [task: check_if_new_proposals] If there's no new messages to send,
call EmptyOperator and finish the DAG.
If there's new messages to send, call [send_telegram_messages].
5. [task: send_telegram_messages] Send messages.
6. [task: save_update_date] Save last telegram message on Airflow Variable.
"""

# pylint: disable=import-error, pointless-statement, expression-not-assigned, invalid-name

import os
import yaml

import time
from datetime import datetime
from typing import Tuple
from urllib.parse import urljoin
import logging

import pendulum
from bs4 import BeautifulSoup
import pandas as pd

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.providers.telegram.hooks.telegram import TelegramHook
from telegram.error import RetryAfter
from tenacity import RetryError

from authenticate_decidim import AuthenticateDecidim

# from airflow_commons.slack_messages import send_slack


DECIDIM_CONN_ID = "api_decidim"
MESSAGE_COOLDOWN_DELAY = 30
MESSAGE_COOLDOWN_RETRIES = 10


class DecidimNotifierDAGGenerator:
    def generate_dag(self, telegram_conn_id: str, process_id: str, start_date: str):
        self.process_id = process_id
        self.telegram_conn_id = telegram_conn_id
        self.most_recent_msg_time = f"most_recent_msg_time_{process_id}"
        self.start_date = datetime.fromisoformat(start_date.isoformat())

        # DAG
        default_args = {
            "owner": "vitor",
            "start_date": self.start_date,
            "depends_on_past": False,
            "retries": 0,
            # "on_failure_callback": send_slack,
            # "on_retry_callback": send_slack,
        }

        @dag(
            dag_id=f"dedicim_notify_new_proposals_{self.process_id}",
            default_args=default_args,
            schedule="*/3 * * * *",  # every 3 minutes
            catchup=False,
            description=__doc__,
            max_active_runs=1,
            tags=["notificação", "decidim"],
        )
        def dedicim_notify_new_proposals():
            def _parse_json_to_df(proposals: dict) -> pd.DataFrame:
                """Parse decidim API json return to a pandas DataFrame.

                Args:
                    proposals (dict): API json return

                Returns:
                    pd.DataFrame: json parsed into pandas DataFrame
                """

                df = pd.DataFrame()
                for participatory_process in proposals["data"][
                    "participatoryProcesses"
                ]:
                    for component in participatory_process["components"]:
                        if component is None or component == {}:
                            continue

                        proposals = component["proposals"]["edges"]
                        df_proposals = pd.json_normalize(proposals)
                        df_proposals["component.id"] = component["id"]
                        df_proposals["component.name"] = component["name"][
                            "translation"
                        ]
                        df_proposals[
                            "participatory_process.name"
                        ] = participatory_process["title"]["translation"]
                        df_proposals[
                            "participatory_process.slug"
                        ] = participatory_process["slug"]
                        df = pd.concat([df, df_proposals], ignore_index=True)

                return df

            def _prepare_strings(
                row: pd.core.series.Series,
            ) -> Tuple[str, str, str, str]:
                """Prepares strings for a given row in a pandas Series to
                compose telegram message.

                Args:
                    row (pd.core.series.Series): a pandas Series representing a
                        row of data.

                Returns:
                    Tuple[str, str, str, str]: A tuple containing the prepared
                        strings in the following order:
                        - header, organization_name, body, link
                """

                organization_name = (
                    f'({row["node.author.organizationName"]})'
                    if row["node.author.organizationName"] != ""
                    and row["node.author.organizationName"] != "Brasil Participativo"
                    else ""
                )

                body = BeautifulSoup(
                    row["node.body.translation"], "html.parser"
                ).get_text()

                decidim_conn_values = BaseHook.get_connection(DECIDIM_CONN_ID)
                link = urljoin(
                    decidim_conn_values.host,
                    f"processes/{row['participatory_process.slug']}/f/{row['component.id']}/proposals/{row['node.id']}",
                )

                published_at = row["node.publishedAt"]
                updated_at = row["node.updatedAt"]

                state = row["node.state"]
                state_map = {
                    "accepted": {"label": "aceita ", "emoji": "✅ ✅ ✅"},
                    "evaluating": {"label": "em avaliação ", "emoji": "📥 📥 📥"},
                    "withdrawn": {"label": "retirada ", "emoji": "🚫 🚫 🚫"},
                    "rejected": {"label": "rejeitada ", "emoji": "⛔ ⛔ ⛔"},
                    "others": {"label": "atualizada ", "emoji": "🔄 🔄 🔄"},
                    "new": {"label": "", "emoji": "📣 📣 📣 <b>[NOVA]</b>"},
                }

                if updated_at > published_at:
                    date = updated_at.strftime("%d/%m/%Y %H:%M")
                    emoji = state_map.get(state, state_map["others"])["emoji"]
                    state_label = state_map.get(state, state_map["others"])["label"]
                else:
                    date = published_at.strftime("%d/%m/%Y %H:%M")
                    emoji = state_map["new"]["emoji"]
                    state_label = state_map["new"]["label"]

                header = f"{emoji} Proposta <b>{state_label}</b>em {date}"

                return header, organization_name, body, link

            @task
            def get_update_date() -> datetime:
                """Airflow task that retrieve last proposal update date from
                airflow variables.

                Returns:
                    datetime: last proposal update from airflow variables.
                """

                date_format = "%Y-%m-%d %H:%M:%S%z"
                update_datetime = Variable.get(self.most_recent_msg_time)

                return datetime.strptime(update_datetime, date_format)

            @task
            def get_proposals(component_id: int, update_date: datetime) -> dict:
                """Airflow task that uses variable `graphiql` to request
                proposals on dedicim API.

                Args:
                    update_date (datetime): last proposals update date.

                Returns:
                    dict: result of decidim API query on proposals.
                """

                graphiql = f"""{{
                    component(id: {component_id}) {{
                        id
                        name {{
                            translation(locale: "pt-BR")
                        }}
                        ... on Proposals {{
                        name {{
                            translation(locale: "pt-BR")
                        }}
                        proposals(filter: {{publishedSince: {update_date}}}, order: {{publishedAt: "desc"}}) {{
                            edges {{
                                node {{
                                    id
                                    title {{
                                        translation(locale: "pt-BR")
                                    }}
                                    publishedAt
                                    updatedAt
                                    state
                                    author {{
                                        name
                                        organizationName
                                    }}
                                    category {{
                                        name {{
                                            translation(locale: "pt-BR")
                                        }}
                                    }}
                                    body {{
                                        translation(locale: "pt-BR")
                                    }}
                                    official
                                        }}
                                    }}
                                }}
                            }}
                        }}
                            decidim {{
                                version
                            }}
                    }}
                """

                decidim_conn_values = BaseHook.get_connection(DECIDIM_CONN_ID)
                api_url = urljoin(decidim_conn_values.host, "api")
                session = AuthenticateDecidim(DECIDIM_CONN_ID).get_session()
                response = session.post(api_url, json={"query": graphiql})
                session.close()

                return response.json()

            @task(multiple_outputs=True)
            def mount_telegram_messages(
                proposals_json: dict, update_date: datetime
            ) -> dict:
                """Airflow task that parse proposals json, select only new or
                updated proposal, get the max proposal date (new or update) and
                mount message for telegram.

                Args:
                    proposals (dict): list of proposals received on function
                        `get_proposals`.
                    update_date (datetime): last proposals update date.

                Returns:
                    dict: "proposals_messages" (list): new/updated proposals to
                            send on telegram.
                        "max_datetime" (str): max proposal date (new or update).
                """
                logging.info(f"Recived proposals {proposals_json}")
                proposals_df = _parse_json_to_df(proposals_json)
                proposals_df.fillna("", inplace=True)
                proposals_df["node.publishedAt"] = pd.to_datetime(
                    proposals_df["node.publishedAt"]
                )
                proposals_df["node.updatedAt"] = pd.to_datetime(
                    proposals_df["node.updatedAt"]
                )

                # filter dataframe to only newer than update_date
                proposals_df_new = proposals_df[
                    (proposals_df["node.publishedAt"] > update_date)
                    | (proposals_df["node.updatedAt"] > update_date)
                ].copy()

                NOT_FOUND_MSG = "-"
                proposals_messages = []
                for _, row in proposals_df_new.iterrows():
                    proposal_title = (
                        row["node.title.translation"]
                        if "node.title.translation" in row
                        else NOT_FOUND_MSG
                    )
                    author_name = (
                        row["node.author.name"]
                        if "node.author.name" in row
                        else NOT_FOUND_MSG
                    )
                    category = (
                        row["node.category.name.translation"]
                        if "node.category.name.translation" in row
                        else NOT_FOUND_MSG
                    )
                    header, organization_name, body, link = _prepare_strings(row)

                    proposal_message = (
                        f"{header}"
                        "\n"
                        "\n<b>Proposta</b>"
                        f"\n{proposal_title}"
                        "\n"
                        f"\n<b>Autor</b>"
                        f"\n{author_name} {organization_name}"
                        "\n"
                        "\n<b>Categoria</b>"
                        f"\n{category}"
                        "\n"
                        f"\n{body}"
                        "\n"
                        f'\n<a href="{link}">Acesse aqui</a>'
                    )
                    proposals_messages.append(proposal_message)

                max_datetime = (
                    proposals_df_new[
                        ["node.updatedAt", "node.publishedAt"]
                    ].values.max()
                    if proposals_messages
                    else None
                )

                return {
                    "proposals_messages": proposals_messages,
                    "max_datetime": max_datetime,
                }

            @task.branch
            def check_if_new_proposals(selected_proposals: list) -> str:
                """Airflow task branch that check if there is new or updated
                proposals to send on telegram.

                Args:
                    selected_proposals (list): list of selected proposals
                        messages to send on telegram.

                Returns:
                    str: next Airflow task to be called
                """

                if selected_proposals["proposals_messages"]:
                    return "send_telegram_messages"
                else:
                    return "skip_send_message"

            @task
            def send_telegram_messages(proposals_messages: list):
                """Airflow task to send telegram messages.

                Args:
                    proposals_messages (list): List of proposals telegram
                        messages to be send.
                """

                for message in proposals_messages:
                    for _ in range(MESSAGE_COOLDOWN_RETRIES):
                        try:
                            TelegramHook(
                                telegram_conn_id=self.telegram_conn_id
                            ).send_message(api_params={"text": message})
                            break
                        except (RetryError, RetryAfter) as e:
                            logging.info("Exception caught: %s", e)
                            logging.warning(
                                "Message refused by Telegram's flood control. "
                                "Waiting %d seconds...",
                                MESSAGE_COOLDOWN_DELAY,
                            )
                            time.sleep(MESSAGE_COOLDOWN_DELAY)

            @task
            def save_update_date(max_datetime: str):
                """Airflow task to update last proposal datetime saved on
                airflow variables.

                Args:
                    max_datetime (str): last proposal datetime
                """

                Variable.set("decidim_proposals_update_datetime", max_datetime)

            # Instantiation
            update_date = get_update_date()
            proposals_json = get_proposals(update_date)
            selected_proposals = mount_telegram_messages(proposals_json, update_date)
            check_if_new_proposals_task = check_if_new_proposals(selected_proposals)

            # Orchestration
            check_if_new_proposals_task >> EmptyOperator(task_id="skip_send_message")
            (
                check_if_new_proposals_task
                >> send_telegram_messages(selected_proposals["proposals_messages"])
                >> save_update_date(selected_proposals["max_datetime"])
            )

        return dedicim_notify_new_proposals()


def read_yaml_files_from_directory():
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    directory_path = os.path.join(cur_dir, "processes_confs")

    for filename in os.listdir(directory_path):
        # Check if the file is a YAML file
        if filename.endswith(".yaml") or filename.endswith(".yml"):
            filepath = os.path.join(directory_path, filename)

            with open(filepath, "r") as file:
                try:
                    yaml_dict = yaml.safe_load(file)
                    DecidimNotifierDAGGenerator().generate_dag(
                        **yaml_dict["process_params"]
                    )

                except yaml.YAMLError as e:
                    print(f"Error reading {filename}: {e}")


read_yaml_files_from_directory()
