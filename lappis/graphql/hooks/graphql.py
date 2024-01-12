"""A class for authenticating with a GraphQL API using Apache Airflow.

This class, GraphQLHook, is designed to authenticate with a GraphQL API using the provided connection ID in the context of Apache Airflow. It extends the BaseHook class and provides methods for executing GraphQL queries.
"""

import logging
from pathlib import Path
from urllib.parse import urljoin
import requests
from airflow.hooks.base import BaseHook
from typing import Optional, Union, Dict, Any, Generator


class GraphQLHook(BaseHook):
    def __init__(self, conn_id: str):
        """
        Initializes the GraphQLHook instance.

        Args:
            conn_id (str): The connection ID used for authentication.
        """
        conn_values = self.get_connection(conn_id)
        self.conn_id = conn_id
        self.api_url = conn_values.host
        self.auth_url = urljoin(self.api_url, "api/sign_in")
        self.payload = {
            "user[email]": conn_values.login,
            "user[password]": conn_values.password,
        }

    def get_graphql_query_from_file(self, path_para_arquivo_query: Union[Path, str]) -> str:
        """
        Reads and returns the contents of a GraphQL query file.

        Args:
            path_para_arquivo_query (Union[Path, str]): The path to the GraphQL query file.

        Returns:
            str: The contents of the GraphQL query file.
        """

        return open(path_para_arquivo_query).read()

    def get_session(self) -> requests.Session:
        """
        Creates a requests session authenticated with the provided user credentials.

        Returns:
            requests.Session: Authenticated session object.
        """
        session = requests.Session()

        try:
            r = session.post(self.auth_url, data=self.payload)
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logging.info("A login error occurred: %s", e)
            raise e
        return session

    def run_graphql_query(self, graphql_query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
        """
        Executes a GraphQL query and returns the JSON response.

        Args:
            graphql_query (str): The GraphQL query to execute.
            variables (Optional[Dict[str, Any]]): Optional variables to include in the query.

        Returns:
            dict: The JSON response of the GraphQL query.
        """
        response = self.get_session().post(
            self.api_url, json={"query": graphql_query, "variables": variables}
        )
        status_code = response.status_code
        assert status_code == 200, logging.ERROR(
            f"""Query:\n\n\t {graphql_query} \n\nhas returned status code: {status_code}"""
        )

        return response.json()

    def run_graphql_paginated_query(self, paginated_query: str, component_type: Optional[str] = None, variables: Optional[Dict[str, Any]] = {}) -> Generator[Dict[str, Any], None, None]:
        """
        Executes a paginated GraphQL query and yields responses.

        Args:
            paginated_query (str): The paginated GraphQL query to execute.
            component_type (str): The type of component in the GraphQL query.
            variables (dict): Optional variables to include in the query.

        Yields:
            dict: The JSON response of each paginated query.
        """
        if "page" not in variables:
            variables = {**variables, "page": "null"}

        response = self.run_graphql_query(paginated_query, variables)

        print(response["data"]["component"])

        variables["page"] = response["data"]["component"][component_type.lower()][
            "pageInfo"
        ]["endCursor"]
        hasnextpage = response["data"]["component"][component_type.lower()]["pageInfo"][
            "hasNextPage"
        ]

        if hasnextpage:
            yield from self.run_graphql_paginated_query(
                paginated_query,
                component_type=component_type,
                variables=variables,
            )

        yield response
