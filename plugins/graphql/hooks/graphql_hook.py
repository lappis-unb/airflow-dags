"""A class for authenticating with a GraphQL API using Apache Airflow.

This class, GraphQLHook, is designed to authenticate with a GraphQL API,
using the provided connection ID in the context of Apache Airflow.
It extends the BaseHook class and provides methods for executing GraphQL queries.
"""

import logging
from contextlib import closing
from os import walk
from pathlib import Path
from typing import Any, Dict, Generator, Optional, Union
from urllib.parse import urljoin

import requests
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection

from plugins.utils.dict_utils import key_lookup


class GraphQLHook(BaseHook):
    """Uma classe para autenticação com uma API GraphQL usando o Apache Airflow.

    Esta classe, GraphQLHook, estende a classe BaseHook e fornece métodos para executar consultas GraphQL.

    Args:
    ----
        conn_id (str): O ID de conexão usado para autenticação.
        api_url (str): A URL base para a API GraphQL.
        auth_url (str): A URL para autenticação na API GraphQL.
        payload (dict): O payload contendo o email e a senha do usuário para autenticação.

    Methods:
    -------
        __init__(self, conn_id: str):
            Inicializa a instância GraphQLHook.

        get_graphql_query_from_file(self, path_para_arquivo_query: Union[Path, str]) -> str:
            Lê e retorna o conteúdo de um arquivo de consulta GraphQL.

        get_session(self) -> requests.Session:
            Cria uma sessão autenticada com as credenciais de usuário fornecidas.

        run_graphql_query(
            self, graphql_query: str, variables: Optional[Dict[str, Any]] = None
        ) -> Dict[str, str]:
            Executa uma consulta GraphQL e retorna a resposta em JSON.

        run_graphql_paginated_query(
            self,
            paginated_query: str,
            component_type: Optional[str] = None,
            variables: Optional[Dict[str, Any]] = None
        ) -> Generator[Dict[str, Any], None, None]:
            Executa uma consulta GraphQL paginada e gera respostas.

        get_components_ids_by_type(self, component_type: str):
            Obtém todos os IDs de componentes filtrados por tipo.
    """

    def __init__(self, conn_id: str):
        """
        Initializes the GraphQLHook instance.

        Args:
        ----
            conn_id (str): The connection ID used for authentication.
        """
        assert isinstance(conn_id, str), "Param type of conn_id has to be str"

        conn_values = self.get_connection(conn_id)
        assert isinstance(conn_values, Connection), "conn_values was not created correctly."

        self.conn_id = conn_id
        self.api_url = conn_values.host
        self.auth_url = urljoin(self.api_url, "api/sign_in")
        self.payload = {
            "user[email]": conn_values.login,
            "user[password]": conn_values.password,
        }

    def get_session(self) -> requests.Session:
        """
        Creates a requests session authenticated with the provided user credentials.

        Returns
        -------
            requests.Session: Authenticated session object.
        """
        session = requests.Session()

        try:
            r = session.post(self.auth_url, data=self.payload, verify=True)
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logging.info("A login error occurred: %s", e)
            raise e
        return session

    @classmethod
    def get_graphql_query_from_file(cls, path_para_arquivo_query: Union[Path, str]) -> str:
        """
        Reads and returns the contents of a GraphQL query file.

        Args:
        ----
            path_para_arquivo_query (Union[Path, str]): The path to the GraphQL query file.

        Returns:
        -------
            str: The contents of the GraphQL query file.
        """
        assert isinstance(
            path_para_arquivo_query, (Path, str)
        ), "Param path_para_arquivo_query has to be one of [str, Path]"
        assert Path(path_para_arquivo_query).exists(), f"Query file: {path_para_arquivo_query}, not found"
        with closing(open(path_para_arquivo_query)) as file:
            return file.read()

    def run_graphql_query(
        self, graphql_query: Union[str, Path], variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, str]:
        """
        Executes a GraphQL query and returns the JSON response.

        Args:
        ----
            graphql_query (str): The GraphQL query to execute or the path to the graphql query.
            variables (Optional[Dict[str, Any]]): Optional variables to include in the query.

        Returns:
        -------
            dict: The JSON response of the GraphQL query.
        """
        try:
            response = self.get_session().post(
                self.api_url, json={"query": graphql_query, "variables": variables}
            )
            response.raise_for_status()
        except requests.HTTPError as exp:
            logging.error("""Query:\n\n\t %s""", graphql_query)
            raise exp

        return response.json()

    def run_graphql_paginated_query(
        self,
        paginated_query: str,
        variables: Optional[Dict[str, Any]] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Executes a paginated GraphQL query and yields responses.

        Args:
        ----
            paginated_query (str): The paginated GraphQL query to execute.
            component_type (str): The type of component in the GraphQL query.
            variables (dict): Optional variables to include in the query.

        Yields:
        ------
            dict: The JSON response of each paginated query.
        """
        if variables is None:
            variables = {}
        if "page" not in variables:
            variables = {**variables, "page": "null"}

        response = self.run_graphql_query(paginated_query, variables)

        page_info = key_lookup(response, "pageInfo")
        has_next_page = None

        if page_info is not None:
            variables["page"] = page_info["endCursor"]
            has_next_page = page_info["hasNextPage"]
        else:
            logging.warning("Não achou a chave page_info.")

        if has_next_page:
            yield from self.run_graphql_paginated_query(
                paginated_query,
                variables=variables,
            )
        logging.info("Page processed: %s.", variables["page"])
        yield response

    def get_components_ids_by_type(self, component_type: str):
        """
        Gets all components id, filtered by type.

        Parameters
        ----------
            component_type(str): Component type.

        Return:
            result(list): List of all components id.
        """
        directory_path = Path(__file__).parent.joinpath("./components")

        result = []
        for path, _, files in walk(directory_path):
            for file in files:
                query_path = f"{path}/{file}"
                query = self.get_graphql_query_from_file(query_path)
                component = self.run_graphql_query(query)
                space_type = next(iter(component["data"].keys()))

                for components in component["data"][space_type]:
                    space_components = components["components"]
                    result.extend([x["id"] for x in space_components if x["__typename"] == component_type])
        return result
