"""
DAG to set proposals availability on decidim. The result is
the checkbox `Participantes podem criar propostas` marked as checked or
not.

When setting decidim's proposals availability:
if proposals_status is true
    then the code `marking` checkbox sends
        (component[step_settings][1][creation_enabled], 0)
        (component[step_settings][1][creation_enabled], 1)
else
    then the code `unmarking` checkbox sends
        (component[step_settings][1][creation_enabled], 0)
"""

# pylint: disable=import-error, invalid-name, expression-not-assigned
import os
import yaml

import time

from datetime import datetime, timedelta
from collections import defaultdict
import bs4
import re
from bs4 import BeautifulSoup
import logging

from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.telegram.hooks.telegram import TelegramHook

from lappis.decidim import DecidimHook
from requests.exceptions import HTTPError
from telegram.error import RetryAfter
from tenacity import RetryError

DECIDIM_CONN_ID = "api_decidim"
PAGE_FORM_CLASS = "form edit_component"


class DecidimOnOffDAGGenerator:
    def generate_dag(
        self,
        telegram_conn_id: str,
        component_id: str,
        process_id: str,
        start_date: str,
        decidim_url: str,
        dag_id: str,
        schedule: str
    ):
        self.component_id = component_id
        self.process_id = process_id
        self.telegram_conn_id = telegram_conn_id
        self.most_recent_msg_time = f"most_recent_msg_time_{process_id}"
        self.start_date = datetime.fromisoformat(start_date.isoformat())
        self.decidim_url = decidim_url

        default_args = {
            "owner": "Paulo G./Thais R.",
            "start_date": datetime(2023, 8, 21),
            "end_date": datetime(2023, 8, 25),
            "depends_on_past": False,
            "retries": 3,
            "retry_delay": timedelta(minutes=1),
            # "on_failure_callback": send_slack,
            # "on_retry_callback": send_slack, #! Change to telegram notifications.
        }

        @dag(
            dag_id=f"{dag_id}_{self.process_id}",
            default_args=default_args,
            schedule=schedule,
            catchup=False,
            description=__doc__,
            tags=["decidim"],
        )

        def decidim_on_n_off_proposals(
            proposals_status: bool,
        ):  # pylint: disable=missing-function-docstring
            # due to Airflow DAG __doc__
    
            def _convert_html_form_to_dict(html_form: bs4.element.Tag) -> defaultdict:
                """Convert html <form> and <input> tags to python dictionary.
    
                Args:
                    html_form (bs4.element.Tag): beautiful soup object with
                        respective html <form> filtered.
    
                Returns:
                    defaultdict: a dictionary of lists with html input tag name
                    and value.
                """
    
                dict_output = defaultdict(list)
                for tag in html_form.find_all("input"):
                    if tag.get("type", None) == "checkbox":
                        if tag.get("checked", None):
                            dict_output[tag["name"]].append(tag["value"])
                    else:
                        dict_output[tag["name"]].append(tag["value"])
    
                return dict_output

            def _find_form_input_id(dict_form: bs4.element.Tag):
                """Find a form input id using regex.

                Args:
                    dict_form (bs4.element.Tag): a dict contains beautiful soup objects
                        with respective html <form> filtered.

                Returns:
                    form_input_id: a string with form input id value.

                Raises:
                    IndexError: If does not found a component of creation enabled.
                """

                pattern = r'component\[step_settings\]\[\d+\]\[creation_enabled\]'
                pattern_match = re.findall(pattern, str(dict_form))

                form_input_id = pattern_match.pop(0)

                logging.info(f"FORM_INPUT_ID: {form_input_id}")

                return form_input_id

            @task
            def set_proposals_availability(proposals_status: bool):
                """Airflow task that uses python requests to set the status
                of html input checkbox `Participantes podem criar propostas`.

                It means that a decidim component became available or unavailable
                to receive new proposals.

                Args:
                    proposals_status (bool): the desired action on the html
                        input checkbox `Participantes podem criar propostas`.
                """

                component_url:str = Variable.get(DECIDIM_URL)
                assert component_url.endswith("/edit")

                session = DecidimHook(DECIDIM_CONN_ID).get_session()

                return_component_page = session.get(f"{component_url}")
                if return_component_page.status_code != 200:
                    raise HTTPError(f"Status code is {return_component_page.status_code} and not 200.")

                b = BeautifulSoup(return_component_page.text, "html.parser")
                html_form = b.find(class_=PAGE_FORM_CLASS)

                dict_form = _convert_html_form_to_dict(html_form)
                form_input_id = _find_form_input_id(dict_form)

                # set proposals availability
                if proposals_status:
                    dict_form[form_input_id] = ["1"]
                else:
                    dict_form[form_input_id] = ["0"]

                data = list(dict_form.items())
                session.post(component_url.rstrip("/edit"), data=data)
                session.close()

            @task
            def send_telegram(proposals_status: bool):
                """Airflow task to send telegram message.

                Args:
                    proposals_status (bool): the desired action on the html
                        input checkbox `Participantes podem criar propostas`.
                """

                if proposals_status:
                    message = "✅ <b>[ATIVADO]</b> \n\n<i>Participantes podem criar propostas</i>"
                else:
                    message = "🚫 <b>[DESATIVADO]</b> \n\n<i>Participantes podem criar propostas</i>"

                TelegramHook(telegram_conn_id=TELEGRAM_CONN_ID).send_message(
                            api_params={"text": message}
                        )

            set_proposals_availability(proposals_status) >> \
            send_telegram(proposals_status)

        return decidim_on_n_off_proposals

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
                    DecidimOnOffDAGGenerator().generate_dag(
                        **yaml_dict["process_params"],
                        dag_id="decidim_set_on_proposals", 
                        schedule="0 8 * * *")(True)

                    DecidimOnOffDAGGenerator().generate_dag(
                        **yaml_dict["process_params"],
                        dag_id="decidim_set_off_proposals",
                        schedule="0 22 * * *")(False)

                except yaml.YAMLError as e:
                    logging.ERROR(f"Error reading {filename}: {e}")

read_yaml_files_from_directory()