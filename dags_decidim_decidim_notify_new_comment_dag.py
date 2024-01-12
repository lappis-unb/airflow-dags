# pylint: disable=import-error, pointless-statement, expression-not-assigned, invalid-name

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Union

import pandas as pd
import yaml
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.telegram.hooks.telegram import TelegramHook
from telegram.error import RetryAfter
from tenacity import RetryError

from lappis.decidim_hook import DecidimHook

DECIDIM_CONN_ID = "api_decidim"
MESSAGE_COOLDOWN_DELAY = 30
MESSAGE_COOLDOWN_RETRIES = 10


class DecidimNotifierDAGGenerator:
    def generate_dag(
        self, telegram_conn_id: str, component_id: str, process_id: str, start_date: str, **kwargs
    ):
        self.component_id = component_id
        self.process_id = process_id
        self.telegram_conn_id = telegram_conn_id
        self.most_recent_msg_time = f"most_recent_comment_time_{process_id}"
        self.start_date = datetime.fromisoformat(start_date.isoformat())

        # DAG
        default_args = {
            "owner": "Paulo",
            "start_date": self.start_date,
            "depends_on_past": False,
            "retries": 0,
            # "on_failure_callback": send_slack,
            # "on_retry_callback": send_slack,
        }

        @dag(
            dag_id=f"dedicim_notify_new_comments_{self.process_id}",
            default_args=default_args,
            schedule="@hourly",  # every 1 hour
            catchup=False,
            description=__doc__,
            max_active_runs=1,
            tags=["notificação", "decidim"],
        )
        def dedicim_notify_new_comments():
            @task
            def get_update_date(dag_start_date: datetime) -> datetime:
                """Airflow task that retrieve last comment update date from
                airflow variables.

                Returns:
                    datetime: last comment update from airflow variables.
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
            def get_comments(component_id: int, update_date: datetime):
                """Airflow task that uses variable `graphql` to request
                comments on dedicim API.

                Args:
                    component_id (int): id of the component to get updates from.
                    update_date (datetime): last comments update date.

                Returns:
                    dict: result of decidim API query on comments.
                """

                msgs_dict = DecidimHook(DECIDIM_CONN_ID, component_id).get_comments(
                    update_date_filter=update_date
                )

                return msgs_dict

            @task(multiple_outputs=True)
            def mount_telegram_messages(mensages_json: dict) -> dict:
                """Airflow task that parse comments json, select only new or
                updated comment, get the max comment date (new or update) and
                mount message for telegram.

                Args:
                    comments (dict): list of comments received on function
                        `get_comments`.
                    update_date (datetime): last comments update date.

                Returns:
                    dict: "comments_messages" (list): new/updated comments to
                            send on telegram.
                        "max_datetime" (str): max comment date (new or update).

                """

                result: dict[str, Union[list, datetime, None]] = {
                    "comments_messages": [],
                    "max_datetime": None,
                }

                df = pd.DataFrame(mensages_json)
                if df.empty:
                    return result

                df["creation_date"] = pd.to_datetime(
                    df["creation_date"], format="ISO8601"
                )

                for _, row in df.iterrows():
                    comment_message = (
                        f"💬💬💬  Novo comentario em {row['creation_date'].strftime('%d/%m/%Y %H:%M')}"
                        "\n"
                        f"\n<b>Autor</b>"
                        f"\n{row['author_name']}"
                        "\n"
                        "\n<b>Comentario</b>"
                        f"\n{row['body']}"
                        "\n"
                        f'\n<a href="{row["link"]}">Acesse aqui</a>'
                    )
                    result["comments_messages"].append(comment_message)

                result["max_datetime"] = df["creation_date"].max()

                logging.info(f"Monted {len(result['comments_messages'])} menssages.")
                return result

            @task.branch
            def check_if_new_comments(selected_comments: list) -> str:
                """Airflow task branch that check if there is new or updated
                comments to send on telegram.

                Args:
                    selected_comments (list): list of selected comments
                        messages to send on telegram.

                Returns:
                    str: next Airflow task to be called
                """

                if selected_comments["comments_messages"]:
                    return "send_telegram_messages"
                else:
                    return "skip_send_message"

            @task
            def send_telegram_messages(comments_messages: list):
                """Airflow task to send telegram messages.

                Args:
                    comments_messages (list): List of comments telegram
                        messages to be send.
                """
                for message in comments_messages:
                    for _ in range(MESSAGE_COOLDOWN_RETRIES):
                        try:
                            hook = TelegramHook(telegram_conn_id=self.telegram_conn_id)
                            #! Possivel code smell ?
                            telegram_topic = hook.get_connection(
                                self.telegram_conn_id
                            ).schema

                            hook.send_message(
                                api_params={
                                    "text": message,
                                    "message_thread_id": telegram_topic,
                                }
                            )

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
                """Airflow task to update last comment datetime saved on
                airflow variables.

                Args:
                    max_datetime (str): last comment datetime
                """

                Variable.set(self.most_recent_msg_time, max_datetime)

            # Instantiation
            update_date = get_update_date(start_date)
            comments_json = get_comments(component_id, update_date)
            selected_comments = mount_telegram_messages(comments_json)
            check_if_new_comments_task = check_if_new_comments(selected_comments)

            # Orchestration
            check_if_new_comments_task >> EmptyOperator(task_id="skip_send_message")
            (
                check_if_new_comments_task
                >> send_telegram_messages(selected_comments["comments_messages"])
                >> save_update_date(selected_comments["max_datetime"])
            )

        return dedicim_notify_new_comments()


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
                    logging.ERROR(f"Error reading {filename}: {e}")


read_yaml_files_from_directory()
