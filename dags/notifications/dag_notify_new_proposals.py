"""DAG to query Decidim software recent proposals and send result to telegram chat.

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

import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Union

# pylint: disable=import-error, pointless-statement, expression-not-assigned, invalid-name

import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Union

import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.telegram.hooks.telegram import TelegramHook
from telegram.error import RetryAfter
from tenacity import RetryError

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

from plugins.decidim_hook import DecidimHook
from plugins.yaml.config_reader import read_yaml_files_from_directory

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.telegram.hooks.telegram import TelegramHook
from telegram.error import RetryAfter
from tenacity import RetryError

from plugins.decidim_hook import DecidimHook
from plugins.yaml.config_reader import read_yaml_files_from_directory

# from airflow_commons.slack_messages import send_slack


DECIDIM_CONN_ID = "api_decidim"
MESSAGE_COOLDOWN_DELAY = 30
MESSAGE_COOLDOWN_RETRIES = 10


class DecidimNotifierDAGGenerator:  # noqa: D101
    def generate_dag(
        self,
        telegram_config: str,
        component_id: str,
        process_id: str,
        start_date: str,
        end_date: str,
        **kwargs,
    ):
        self.component_id = component_id
        self.process_id = process_id

        self.telegram_conn_id = telegram_config["telegram_conn_id"]
        self.telegram_chat_id = telegram_config["telegram_group_id"]
        self.telegram_topic_id = telegram_config[
            "telegram_moderation_proposals_topic_id"
        ]

        self.most_recent_msg_time = f"most_recent_msg_time_{process_id}"
        self.start_date = (
            start_date
            if isinstance(start_date, str)
            else start_date.strftime("%Y-%m-%d")
        )
        if end_date is not None:
            self.end_date = (
                end_date if isinstance(end_date, str) else end_date.strftime("%Y-%m-%d")
            )
        else:
            self.end_date = end_date
        # DAG
        default_args = {
            "owner": "Paulo G./Thais R.",
            "start_date": self.start_date,
            "end_date": self.end_date,
            "depends_on_past": False,
            "retries": 0,
            # "on_failure_callback": send_slack,
            # "on_retry_callback": send_slack,
        }

        @dag(
            dag_id=f"notify_new_proposals_{self.process_id}",
            default_args=default_args,
            schedule="*/3 * * * *",  # every 3 minutes
            catchup=False,
            description=__doc__,
            max_active_runs=1,
            tags=["notificação", "decidim"],
            is_paused_upon_creation=False,
        )
        def notify_new_proposals():
            @task
            def get_update_date(dag_start_date: datetime) -> datetime:
                """Airflow task that retrieve last proposal update date from airflow variables.

                Returns
                -------
                    datetime: last proposal update from airflow variables.
                """
                date_format = "%Y-%m-%d %H:%M:%S%z"

                tz = timezone(timedelta(hours=-3))
                start_date = datetime(
                    dag_start_date.year,
                    dag_start_date.month,
                    dag_start_date.day,
                    tzinfo=tz,
                ).strftime(date_format)
                update_datetime = Variable.get(self.most_recent_msg_time, start_date)
                return datetime.strptime(update_datetime, date_format)

            @task
            def get_proposals(component_id: int, update_date: datetime):
                """Airflow task that uses variable `graphql` to request proposals on dedicim API.

                Args:
                ----
                    component_id (int): id of the component to get updates from.
                    update_date (datetime): last proposals update date.

                Returns:
                -------
                    dict: result of decidim API query on proposals.
                """
                component_dict = DecidimHook(
                    DECIDIM_CONN_ID, component_id=component_id
                ).get_component(update_date_filter=update_date)

                return component_dict

            @task(multiple_outputs=True)
            def mount_telegram_messages(
                component_id, proposals_json: dict, update_date: datetime
            ) -> dict:
                """Airflow task that parse proposals json, to mount telegram messages.

                Args:
                ----
                    proposals (dict): list of proposals received on function
                        `get_proposals`.
                    update_date (datetime): last proposals update date.

                Returns:
                -------
                    dict: "proposals_messages" (list): new/updated proposals to
                            send on telegram.
                        "max_datetime" (str): max proposal date (new or update).

                """
                logging.info("Recived proposals %s", proposals_json)
                result: dict[str, Union[list, datetime, None]] = {
                    "proposals_messages": [],
                    "max_datetime": None,
                }

                proposals_df = DecidimHook(
                    DECIDIM_CONN_ID, component_id
                ).component_json_to_dataframe(proposals_json)
                if proposals_df.empty:
                    return result

                # filter dataframe to only newer than update_date
                proposals_df_new = proposals_df[
                    (proposals_df["publishedAt"] > update_date)
                    | (proposals_df["updatedAt"] > update_date)
                ].copy()

                for _, row in proposals_df_new.iterrows():
                    state = row["state"]

                    organization_name = row.get("author.organizationName", "")
                    author_name = row.get("author.name", "-")
                    formated_date = row["date"].strftime("%d/%m/%Y %H:%M")
                    proposal_message = (
                        f"{state['emoji']} Proposta <b>{state['label']}</b>em {formated_date}"
                        "\n"
                        "\n<b>Proposta</b>"
                        f"\n{row['title.translation']}"
                        "\n"
                        f"\n<b>Autor</b>"
                        f"\n{author_name} {organization_name}"
                        "\n"
                        "\n<b>Categoria</b>"
                        f"\n{row['category']}"
                        "\n"
                        "\n<b>Descrição</b>"
                        f"\n{row['body.translation']}"
                        "\n"
                        f'\n<a href="{row["link"]}">Acesse aqui</a>'
                    )
                    result["proposals_messages"].append(proposal_message)

                result["max_datetime"] = proposals_df_new["date"].max()

                logging.info("Built %s menssages.", len(result["proposals_messages"]))
                return result

            @task.branch
            def check_if_new_proposals(selected_proposals: list) -> str:
                """Airflow task branch that check if there is new or updated proposals to send on telegram.

                Args:
                ----
                    selected_proposals (list): list of selected proposals
                        messages to send on telegram.

                Returns:
                -------
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
                ----
                    proposals_messages (list): List of proposals telegram
                        messages to be send.
                """
                for message in proposals_messages:
                    for _ in range(MESSAGE_COOLDOWN_RETRIES):
                        try:
                            TelegramHook(
                                telegram_conn_id=self.telegram_conn_id,
                                chat_id=self.telegram_chat_id,
                            ).send_message(
                                api_params={
                                    "text": message,
                                    "message_thread_id": self.telegram_topic_id,
                                }
                            )
                            break
                        except (RetryError, RetryAfter) as e:
                            logging.info("Exception caught: %s", e)
                            logging.warning(
                                "Message refused by Telegram's flood control. Waiting %d seconds...",
                                MESSAGE_COOLDOWN_DELAY,
                            )
                            time.sleep(MESSAGE_COOLDOWN_DELAY)

            @task
            def save_update_date(max_datetime: str):
                """Airflow task to update last proposal datetime saved on airflow variables.

                Args:
                ----
                    max_datetime (str): last proposal datetime
                """
                Variable.set(self.most_recent_msg_time, max_datetime)

            # Instantiation
            update_date = get_update_date(start_date)
            proposals_json = get_proposals(component_id, update_date)
            selected_proposals = mount_telegram_messages(
                component_id, proposals_json, update_date
            )
            check_if_new_proposals_task = check_if_new_proposals(selected_proposals)

            # Orchestration
            check_if_new_proposals_task >> EmptyOperator(task_id="skip_send_message")
            (
                check_if_new_proposals_task
                >> send_telegram_messages(selected_proposals["proposals_messages"])
                >> save_update_date(selected_proposals["max_datetime"])
            )

        return notify_new_proposals()


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


for config in _get_all_config_files():
    if not config["telegram_config"]["telegram_group_id"]:
        continue
    DecidimNotifierDAGGenerator().generate_dag(**config)
