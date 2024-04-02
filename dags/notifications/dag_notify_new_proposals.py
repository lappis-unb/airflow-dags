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

# pylint: disable=import-error, pointless-statement, expression-not-assigned, invalid-name

# pylint: disable=import-error, pointless-statement, expression-not-assigned, invalid-name
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Union

import pandas as pd
from plugins.notifications.base_dag import NotifierDAG

from plugins.decidim_hook import DecidimHook
from plugins.yaml.config_reader import read_yaml_files_from_directory

DECIDIM_CONN_ID = "api_decidim"
MESSAGE_COOLDOWN_DELAY = 30
MESSAGE_COOLDOWN_RETRIES = 10


class NotifyNewProposals(NotifierDAG):  # noqa: D101
    def _get_data(self, component_id: int, update_date: datetime):
        """Airflow task that uses variable `graphql` to request proposals on dedicim API.

        Args:
        ----
            component_id (int): id of the component to get updates from.
            update_date (datetime): last proposals update date.

        Returns:
        -------
            dict: result of decidim API query on proposals.
        """
        component_dict = DecidimHook(DECIDIM_CONN_ID, component_id=component_id).get_component(
            update_date_filter=update_date
        )

        return component_dict

    def _format_telegram_message(self, data_row: pd.Series):
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

        proposals_df = DecidimHook(DECIDIM_CONN_ID, component_id).component_json_to_dataframe(proposals_json)
        if proposals_df.empty:
            return result

        # filter dataframe to only newer than update_date
        proposals_df_new = proposals_df[
            (proposals_df["publishedAt"] > update_date) | (proposals_df["updatedAt"] > update_date)
        ].copy()

        proposals_df_new["telegram_msgs"] = proposals_df_new.apply(self._format_telegram_message, axis=1)

        result["max_datetime"] = proposals_df_new["date"].max()

        logging.info("Built %s menssages.", len(proposals_df_new["telegram_msgs"]))
        return proposals_df_new["telegram_msgs"]


CONFIG_FOLDER = Path(os.environ["AIRFLOW_HOME"] / Path("dags-data/Notifications-Configs"))
for config in read_yaml_files_from_directory(CONFIG_FOLDER):
    if not config["telegram_config"]["telegram_group_id"]:
        continue
    config.pop("decidim_url")
    NotifyNewProposals(notifier_type="Proposals", owners="Paulo/Thais G.", **config).generate_dag()
