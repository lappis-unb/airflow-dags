import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Union

import pandas as pd

from plugins.decidim_hook import DecidimHook
from plugins.notifications.base_dag import NotifierDAG, NotifierTypes
from plugins.yaml.config_reader import read_yaml_files_from_directory

DECIDIM_CONN_ID = "api_decidim"
MESSAGE_COOLDOWN_DELAY = 30
MESSAGE_COOLDOWN_RETRIES = 10


class NotifyNewComments(NotifierDAG):
    """
    A DAG to notify new or updated comments through Telegram.

    This DAG periodically checks for new or updated comments on Decidim API and sends notifications through Telegram.

    """  # noqa: E501

    def _get_data(self, component_id: int, update_date: datetime):
        """Retrieve comments data from Decidim for a given component and update date.

        Args:
        ----
            component_id (int): The ID of the Decidim component to retrieve comments for.
            update_date (datetime): The start date for retrieving comments.

        Returns:
        -------
            dict: A dictionary containing comments data retrieved from Decidim.

        """
        logging.info("Start date for comments: %s", update_date)
        msgs_dict = DecidimHook(DECIDIM_CONN_ID, component_id).get_comments(start_date=update_date)

        return msgs_dict

    def _format_telegram_message(self, data_row: pd.Series):
        """Format a Telegram message based on the provided data row.

        Args:
        ----
            data_row (pd.Series): A pandas Series containing data related to the comment.

        Returns:
        -------
            str: A formatted Telegram message.

        """
        state_map = {
            "update": {"label": "Comentario atualizado", "emoji": "🔄 🔄 🔄"},
            "new": {"label": "Novo comentario", "emoji": "💬"},
        }
        get_state = lambda update_date, creation_date: (
            state_map.get("update") if update_date > creation_date else state_map.get("new")
        )

        state = get_state(data_row["update_date"], data_row["creation_date"])
        date_str = data_row["date_filter"].strftime("%d/%m/%Y %H:%M")
        author_name = data_row["author_name"]
        body = data_row["body"]
        link = data_row["link"]

        comment_message = (
            f"{state['emoji']} {state['label']}: {date_str}\n"
            f"\n<b>Autor</b>\n{author_name}\n"
            f"\n<b>Comentario</b>\n{body}\n"
            f'\n<a href="{link}">Acesse aqui</a>'
        )
        return comment_message

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
            dict: The built Telegram message.
        """
        df = pd.DataFrame(data_json)
        if df.empty:
            return {
                "data": [],
                "max_datetime": None,
            }

        result: dict[str, Union[pd.DataFrame, datetime]] = {
            "data": df.apply(self._format_telegram_message, axis=1),
            "max_datetime": df["date_filter"].max().strftime("%Y-%m-%d %H:%M:%S%z"),
        }

        logging.info("Monted %s menssages.", len(result["data"]))
        return result


CONFIG_FOLDER = Path(os.environ["AIRFLOW_HOME"] / Path("dags-data/Notifications-Configs"))
for config in read_yaml_files_from_directory(CONFIG_FOLDER):
    if not config["telegram_config"]["telegram_group_id"]:
        continue
    config.pop("decidim_url")
    NotifyNewComments(notifier_type=NotifierTypes.COMMENTS, owners="Paulo G.", **config).generate_dag()
