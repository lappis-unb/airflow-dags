"""Class to authenticate at decidim with airflow connection host, login
and password.
"""

# pylint: disable=invalid-name

import logging
from urllib.parse import urljoin
import requests
from contextlib import closing
from airflow.hooks.base import BaseHook
import pandas as pd
import re
from inflection import underscore
import inflect
import numpy as np
from datetime import datetime
from bs4 import BeautifulSoup


class DecidimHook(BaseHook):
    def __init__(self, conn_id: str):
        conn_values = self.get_connection(conn_id)
        self.api_url = conn_values.host
        self.auth_url = urljoin(self.api_url, "api/sign_in")
        self.payload = {
            "user[email]": conn_values.login,
            "user[password]": conn_values.password,
        }

    def run_graphql_post_query(self, graphql_query):
        response = self.get_session().post(self.api_url, json={"query": graphql_query})
        status_code = response.status_code
        assert status_code == 200, logging.ERROR(
            f"""Query:
                                                        {graphql_query}
                                                     has returned status code: {status_code}
                                                    """
        )

        return response.json()
    def get_component_type(self, component_id: str) -> str:
        logging.info(f"Component id: {component_id}")
        graphql_query = f"""
                    {{
                        component(id: {component_id}) {{
                            __typename
                        }}
                    }}
                    """
        response = self.run_graphql_post_query(graphql_query=graphql_query)

        assert response["data"]["component"] is not None
        return response["data"]["component"]["__typename"]
