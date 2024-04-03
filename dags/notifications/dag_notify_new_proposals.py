import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Union

from pandas import Series

from plugins.decidim_hook import DecidimHook
from plugins.notifications.base_dag import NotifierDAG, NotifierTypes
from plugins.yaml.config_reader import read_yaml_files_from_directory

DECIDIM_CONN_ID = "api_decidim"
MESSAGE_COOLDOWN_DELAY = 30
MESSAGE_COOLDOWN_RETRIES = 10


class NotifyNewProposals(NotifierDAG):
    """
    A class to create a DAG for notifying about new or updated proposals via Telegram.

    Inherits from NotifierDAG.

    """

    def _get_data(self, component_id: int, update_date: datetime):
        """
        Retrieves data from Decidim API for the specified component and update date.

        Args:
        ----
            component_id (int): The ID of the component to get updates from.
            update_date (datetime): The last update date.

        Returns:
        -------
            dict: The data retrieved from the Decidim API.
        """
        component_dict = DecidimHook(DECIDIM_CONN_ID, component_id=component_id).get_component(
            update_date_filter=update_date
        )

        return component_dict

    def _format_telegram_message(self, data_row: Series):
        state = data_row["state"]
        organization_name = data_row.get("author.organizationName", "")
        author_name = data_row.get("author.name", "-")
        formated_date = data_row["date"].strftime("%d/%m/%Y %H:%M")

        return (
            f"{state['emoji']} Proposta <b>{state['label']}</b>em {formated_date}\n\n"
            f"<b>Proposta</b>\n{data_row['title.translation']}\n\n"
            f"<b>Autor</b>\n{author_name} {organization_name}\n\n"
            f"<b>Categoria</b>\n{data_row['category']}\n\n"
            f"<b>Descrição</b>\n{data_row['body.translation']}\n\n"
            f'<a href="{data_row["link"]}">Acesse aqui</a>'
        )

    def _build_telegram_message(self, component_id: int, proposals_json: dict, update_date: datetime):
        """
        Build Telegram messages based on proposals JSON.

        Args:
        ----
            component_id (int): The ID of the component.
            proposals_json (dict): The JSON data of proposals.
            update_date (datetime): The update date.

        Returns:
        -------
            dict: The built Telegram message.
        """
        logging.info("Recived proposals %s", proposals_json)
        result: dict[str, Union[list, datetime, None]] = {
            "data": [],
            "max_datetime": None,
        }

        proposals_df = DecidimHook(DECIDIM_CONN_ID, component_id).component_json_to_dataframe(proposals_json)
        if proposals_df.empty:
            return result

        # filter dataframe to only newer than update_date
        proposals_df_new = proposals_df[
            (proposals_df["publishedAt"] > update_date) | (proposals_df["updatedAt"] > update_date)
        ].copy()

        result["data"] = proposals_df_new.apply(self._format_telegram_message, axis=1)
        result["max_datetime"] = proposals_df_new["date"].max()

        logging.info("Built %s menssages.", len(result["data"]))
        return result


CONFIG_FOLDER = Path(os.environ["AIRFLOW_HOME"] / Path("dags-data/Notifications-Configs"))
for config in read_yaml_files_from_directory(CONFIG_FOLDER):
    if not config["telegram_config"]["telegram_group_id"]:
        continue
    config.pop("decidim_url")
    NotifyNewProposals(
        notifier_type=NotifierTypes.PROPOSALS, owners="Paulo G./Thais R.", **config
    ).generate_dag()
