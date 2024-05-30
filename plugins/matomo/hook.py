import logging
from urllib.parse import quote_plus, urlencode

import requests
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection


class MatomoHook(BaseHook):
    """A custom Airflow hook for interacting with the Matomo API.

    This hook provides methods for making secure requests to the Matomo API,
    including bulk requests and individual requests with specified parameters.

    Attributes:
    -----------
    conn_id : str
        The connection ID used for authentication.
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
        self.site_id = conn_values.login
        self.token_auth = conn_values.password

    def _run_secure_request(self, request_data: dict) -> str:
        """
        Makes a secure POST request to the Matomo API.

        Args:
        ----
            request_data (dict): The data to be included in the POST request.

        Returns:
        -------
            str: The response text from the Matomo API.
        """
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        request_data["token_auth"] = self.token_auth
        request_data["idSite"] = self.site_id

        response = requests.post(self.api_url, data=request_data, headers=headers)
        response.raise_for_status()

        return response.text

    def secure_bulk_request(
        self, params_mappings: list[dict[str, str]], response_format: str = "json"
    ) -> str:
        """
        Makes a bulk request to the Matomo API.

        Args:
        ----
            params_mappings (list[dict[str, str]]): A list of parameter mappings for the bulk request.
            response_format (str, optional): The desired response format (default is "json").

        Returns:
        -------
            str: The response text from the Matomo API.
        """
        urls = {
            f"urls[{index}]": quote_plus(urlencode(params)) for index, params in enumerate(params_mappings)
        }
        logging.info("Requesting %s urls.", len(urls.keys()))
        request_param = {
            "module": "API",
            "method": "API.getBulkRequest",
            "format": response_format,
            **urls,
        }

        response = self._run_secure_request(request_data=request_param)
        assert response is not None

        return response

    def secure_request(self, module: str, method: str, filters: dict[str, str], response_format: str) -> str:
        """
        Makes a secure request to the Matomo API.

        Args:
        ----
            module (str): The API module to be accessed.
            method (str): The specific method within the module to be called.
            filters (dict[str, str]): A dictionary of filters to be applied to the request.
            response_format (str): The desired response format.

        Returns:
        -------
            str: The response text from the Matomo API.
        """
        request_param = {
            "module": "API",
            "format": response_format,
            "method": f"{module}.{method}",
            **filters,
        }
        response = self._run_secure_request(request_data=request_param)
        assert response is not None

        return response
