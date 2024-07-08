"""
A DAG (Directed Acyclic Graph) to notify new or updated data through Telegram.

This DAG periodically checks for new or updated data and sends notifications through Telegram.

Args:
----
    owners (str): Owner of the DAG.
    notifier_type (str): Type of notifier, e.g., "Proposal".
    telegram_config (str): Configuration details for Telegram, including `telegram_conn_id`, `telegram_group_id`, and `telegram_moderation_proposals_topic_id`.
    component_id (str): ID of the component.
    process_id (str): ID of the process.
    start_date (str): Start date of the DAG.
    end_date (str): End date of the DAG.

"""  # noqa: E501

import logging
from datetime import datetime
from enum import Enum
from typing import Union

import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.telegram.hooks.telegram import TelegramHook

from plugins.telegram.decorators import telegram_retry


class NotifierTypes(Enum):
    """Enumeration defining different types of notifiers."""

    COMMENTS = "comments"
    PROPOSALS = "proposals"


class NotifierDAG:
    """
    Class to create a notification DAG for notifying new or updated proposals through Telegram.

    Attributes:
    ----------
        owners (str): Owner of the DAG.
        notifier_type (str): Type of notifier, e.g., "Proposal".
        telegram_config (str): Configuration details for Telegram.
        component_id (str): ID of the component.
        process_id (str): ID of the process.
        start_date (str): Start date of the DAG.
        end_date (str): End date of the DAG.
    """

    def __init__(
        self,
        owners: str,
        notifier_type: NotifierTypes,
        telegram_config: dict,
        component_id: str,
        process_id: str,
        start_date: str,
        end_date: str,
    ) -> None:
        assert isinstance(notifier_type, NotifierTypes)

        self.notifier_type = notifier_type
        self.component_id = component_id
        self.process_id = process_id

        self.telegram_conn_id = telegram_config["telegram_conn_id"]
        self.telegram_chat_id = telegram_config["telegram_group_id"]
        self.telegram_topic_id = telegram_config[f"telegram_moderation_{self.notifier_type.value}_topic_id"]

        self.most_recent_msg_time = f"most_recent_{self.notifier_type.value}_time_{process_id}"
        self.start_date = self._format_date(start_date)
        self.end_date = self._format_date(end_date)

        self.default_args = {
            "owner": owners,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "depends_on_past": False,
            "retries": 0,
            # "on_failure_callback": send_slack,
            # "on_retry_callback": send_slack,
        }

    def _format_date(self, date: Union[str, datetime]):
        """
        Format the input date string or datetime object into a standard string format.

        Args:
        ----
            date (Union[str, datetime]): The input date to be formatted.

        Returns:
        -------
            str: The formatted date string.
        """
        if date is None:
            return None

        assert isinstance(date, (str, datetime))
        if isinstance(date, str):
            return date
        if isinstance(date, (datetime)):
            return date.strftime("%Y-%m-%d")

    def _get_update_date(self, dag_start_date: str):
        """
        Get the update date from Airflow variables or the DAG start date.

        Args:
        ----
            dag_start_date (str): The start date of the DAG.

        Returns:
        -------
            pendulum.DateTime: The update date.
        """
        assert isinstance(dag_start_date, str)

        try:
            update_datetime = Variable.get(self.most_recent_msg_time)
            assert isinstance(update_datetime, str)
            return pendulum.parse(str(update_datetime))

        except KeyError:
            date = pendulum.parse(dag_start_date)

        return date

    @telegram_retry(max_retries=20)
    def _send_telegram_notification(self, telegram_conn_id, telegram_chat_id, telegram_topic_id, message):
        """
        Send a notification message through Telegram.

        Args:
        ----
            telegram_conn_id (str): The connection ID for Telegram.
            telegram_chat_id (str): The chat ID for Telegram.
            telegram_topic_id (str): The topic ID for Telegram.
            message (str): The message to be sent.
        """
        TelegramHook(
            telegram_conn_id=telegram_conn_id,
            chat_id=telegram_chat_id,
        ).send_message(
            api_params={
                "text": message,
                "message_thread_id": telegram_topic_id,
            }
        )

    def _get_data(self, component_id: int, update_date: datetime):
        """
        Fetch data based on the component ID and update date.

        Args:
        ----
            component_id (int): The ID of the component.
            update_date (datetime): The update date.

        Returns:
        -------
            pd.DataFrame: The fetched data.
        """
        raise NotImplementedError

    def _format_telegram_message(self, data_row: pd.Series):
        """
        Format a data row into a Telegram message.

        Args:
        ----
            data_row (pd.Series): The data row to be formatted.

        Returns:
        -------
            str: The formatted Telegram message.
        """
        raise NotImplementedError

    def _build_telegram_message(self, component_id: int, data_json: dict, update_date: datetime):
        """
        Build a Telegram message based on the component ID, data JSON, and update date.

        Args:
        ----
            component_id (int): The ID of the component.
            data_json (dict): The JSON data.
            update_date (datetime): The update date.

        Returns:
        -------
            str: The built Telegram message.
        """
        raise NotImplementedError

    def generate_dag(self):
        """
        Generate the notification DAG.

        Returns:
        -------
            airflow.models.DAG: The generated DAG object.
        """

        @dag(
            dag_id=f"notify_new_{self.notifier_type.value.lower()}_{self.process_id}",
            default_args=self.default_args,
            schedule="*/3 * * * *",  # every 3 minutes
            catchup=False,
            description=__doc__,
            max_active_runs=1,
            tags=["notificação", "decidim", self.notifier_type.value.lower()],
            is_paused_upon_creation=False,
        )
        def notify_dag():
            @task
            def get_update_date(dag_start_date: datetime):
                """
                Get the update date for the DAG.

                Args:
                ----
                    dag_start_date (datetime): The start date of the DAG.

                Returns:
                -------
                    pendulum.DateTime: The update date.
                """
                return self._get_update_date(dag_start_date)

            @task(task_id=f"get_{self.notifier_type.value.lower()}")
            def get_data(component_id: int, update_date: datetime):
                """
                Get data for the specified component and update date.

                Args:
                ----
                    component_id (int): The ID of the component.
                    update_date (datetime): The update date.

                Returns:
                -------
                    pd.DataFrame: The fetched data.
                """
                return self._get_data(component_id, update_date)

            @task(multiple_outputs=True)
            def mount_telegram_messages(component_id, data_json: dict, update_date: datetime) -> dict:
                """
                Mount Telegram messages based on the component ID, data JSON, and update date.

                Args:
                ----
                    component_id (int): The ID of the component.
                    data_json (dict): The JSON data.
                    update_date (datetime): The update date.

                Returns:
                -------
                    dict: The mounted Telegram messages.
                """
                return self._build_telegram_message(component_id, data_json, update_date)

            @task.branch
            def check_if_new_data(selected_data: dict[str, Union[pd.Series, list, datetime]]) -> str:
                """
                Check if there are new or updated proposals to send on Telegram.

                Args:
                ----
                    selected_data (dict[str, Union[pd.Series, list, datetime]]): The selected data.

                Returns:
                -------
                    str: The next Airflow task to be called.
                """
                if len(selected_data["data"]):
                    return "send_telegram_messages"
                return "skip_send_message"

            @task
            def send_telegram_messages(proposals_messages: list):
                """
                Send Telegram messages.

                Args:
                ----
                    proposals_messages (list): List of proposal Telegram messages to be sent.
                """
                for menssage in proposals_messages:
                    self._send_telegram_notification(
                        self.telegram_conn_id, self.telegram_chat_id, self.telegram_topic_id, menssage
                    )

            @task
            def save_update_date(max_datetime: str):
                """
                Update the last proposal datetime saved on Airflow variables.

                Args:
                ----
                    max_datetime (str): The last proposal datetime.
                """
                logging.info(max_datetime)
                if max_datetime is not None:
                    Variable.set(self.most_recent_msg_time, max_datetime)

            # Instantiation
            update_date = get_update_date(self.start_date)
            data_json = get_data(self.component_id, update_date)
            selected_proposals = mount_telegram_messages(self.component_id, data_json, update_date)
            check_if_new_proposals_task = check_if_new_data(selected_proposals)

            # Orchestration
            check_if_new_proposals_task >> EmptyOperator(task_id="skip_send_message")
            (
                check_if_new_proposals_task
                >> send_telegram_messages(selected_proposals["data"])
                >> save_update_date(selected_proposals["max_datetime"])
            )

        return notify_dag()
