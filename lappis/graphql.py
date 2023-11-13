"""Class to authenticate in a graphql api
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
from collections import defaultdict

from json import loads
from pathlib import Path


class GraphQLHook(BaseHook):
    def __init__(self, conn_id: str):
        conn_values = self.get_connection(conn_id)
        self.api_url = conn_values.host
        self.auth_url = urljoin(self.api_url, "api/sign_in")
        self.payload = {
            "user[email]": conn_values.login,
            "user[password]": conn_values.password,
        }

    def get_graphql_query_from_file(self, path_para_arquivo_query: Path | str):
        return open(path_para_arquivo_query).read()

    def get_session(self) -> requests.Session:
        """Create a requests session with decidim based on Airflow
        connection host, login and password values.

        Returns:
            requests.Session: session object authenticaded.
        """
        session = requests.Session()

        try:
            r = session.post(self.auth_url, data=self.payload)
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logging.info("An login error occurred: %s", str(e))
        else:
            return session

    def run_graphql_query(self, graphql_query, variables: dict[str] = {}) -> dict[str, str]:
        """
            Execute a GraphQL query with specified variables and retrieve the JSON response.

            Args:
                graphql_query (str): The GraphQL query to execute.
                variables (dict[str, str], optional): Additional variables to include in the query. Defaults to an empty dictionary.

            Returns:
                dict[str, str]: A dictionary containing the JSON response.

            Raises:
                AssertionError: If the HTTP status code is not 200 (OK), an error is logged.

            Example:
                To execute a GraphQL query with variables and retrieve the response:
                >>> response = self.run_graphql_query(query, variables)
                >>> process_response_data(response)

            Note:
                This function sends a POST request to the specified API URL with the provided GraphQL query and variables.
                It checks if the HTTP status code is 200 (OK) and raises an AssertionError if not.

        """
        response = self.get_session().post(
            self.api_url, json={"query": graphql_query, "variables": variables}
        )
        status_code = response.status_code
        assert status_code == 200, logging.ERROR(
            f"""Query:\n\n\t {graphql_query} \n\nhas returned status code: {status_code}"""
        )

        return response.json()

    def run_graphql_pagineted_query(
        self, pagineted_graphql_query: str, variables: dict[str] = {}
    ):
        """
            Execute a paginated GraphQL query and retrieve results page by page.

            Args:
                paginated_graphql_query (str): The GraphQL query to execute for pagination.
                variables (dict[str, str], optional): Additional variables to include in the query. Defaults to an empty dictionary.

            Yields:
                dict: A dictionary containing the JSON response for each page.

            Note:
                This function is designed to handle paginated GraphQL queries. It starts with the provided 'page' variable
                and retrieves pages of results until there are no more pages. The results are yielded page by page.

            Example:
                To fetch paginated results:
                >>> for page in self.run_graphql_paginated_query(query, variables={"page": "some_cursor"}):
                ...     process_page_data(page)

        """
        if "page" not in variables:
            variables = {**variables, "page": "null"}

        response = self.run_graphql_query(pagineted_graphql_query, variables)

        # TODO: Trocar para uma forma mais geral e não apenas proposals.
        variables["page"] = response["data"]["component"]["proposals"]["pageInfo"][
            "endCursor"
        ]
        hasnextpage = response["data"]["component"]["proposals"]["pageInfo"][
            "hasNextPage"
        ]

        if hasnextpage:
            yield from self.run_graphql_post_pagineted_query(
                pagineted_graphql_query, variables=variables
            )

        yield response
