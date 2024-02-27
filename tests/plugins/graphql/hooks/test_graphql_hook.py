from unittest.mock import MagicMock, Mock

import pytest
import requests
from airflow.exceptions import AirflowNotFoundException
from airflow.models.connection import Connection

from plugins.graphql.hooks.graphql_hook import GraphQLHook


@pytest.fixture
def mock_graphql_connection(mocker):
    mock_connection = Connection(
        conn_type="http",
        login="lappis",
        password="lappis_graphql",
        host="https://lab-decide.dataprev.gov.br/",
        port=80,
    )

    mock_connection_uri = mock_connection.get_uri()
    mocker.patch.dict("os.environ", AIRFLOW_CONN_GRAPH_QL_CONN=mock_connection_uri)


def test_success_create_grahpql_hook(mock_graphql_connection):
    hook = GraphQLHook("graph_ql_conn")
    assert hook.conn_id == "graph_ql_conn"
    assert hook.api_url == "https://lab-decide.dataprev.gov.br/"
    assert hook.auth_url == "https://lab-decide.dataprev.gov.br/api/sign_in"
    assert hook.payload == {
        "user[email]": "lappis",
        "user[password]": "lappis_graphql",
    }


@pytest.mark.parametrize("conn_id", [(12), (None), ([])])
def test_fail_create_grahpql_hook(conn_id):
    with pytest.raises(AssertionError):
        GraphQLHook(conn_id)


def test_fail_create_grahpql_hook_conn_not_exists():
    with pytest.raises(AirflowNotFoundException):
        GraphQLHook("not_exists")


@pytest.fixture
def graphql_query_file(tmp_path):
    query_content = "query { example { field } }"
    query_file = tmp_path / "test_query.graphql"
    query_file.write_text(query_content)
    return query_file


def test_get_graphql_query_from_file_existing_file(graphql_query_file):
    result = GraphQLHook.get_graphql_query_from_file(graphql_query_file)
    assert result == "query { example { field } }"


def test_get_graphql_query_from_file_non_existing_file(tmp_path):
    non_existing_file = tmp_path / "non_existing_query.graphql"
    with pytest.raises(AssertionError, match="not found"):
        GraphQLHook.get_graphql_query_from_file(non_existing_file)


def test_get_graphql_query_from_file_invalid_input_type():
    with pytest.raises(AssertionError):
        GraphQLHook.get_graphql_query_from_file(123)


@pytest.fixture
def mock_session(mock_graphql_connection):
    # Mocking the requests.Session object to avoid actual HTTP requests
    return MagicMock(spec=requests.Session)


@pytest.fixture
def mock_requests_session_post(mocker):
    return mocker.patch("requests.Session.post")


def test_successful_authentication(mock_requests_session_post, mocker, mock_graphql_connection):
    mock_response = Mock()
    mock_response.raise_for_status.return_value = None
    mock_requests_session_post.return_value = mock_response

    your_instance = GraphQLHook("graph_ql_conn")
    result = your_instance.get_session()
    assert isinstance(result, requests.Session)


def test_get_session_failed_authentication(mock_requests_session_post, mocker, mock_graphql_connection):
    # Arrange
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Mocked HTTPError")
    mock_requests_session_post.return_value = mock_response

    your_instance = GraphQLHook("graph_ql_conn")
    with pytest.raises(requests.exceptions.HTTPError):
        your_instance.get_session()


def test_get_session_network_error(mock_requests_session_post, mocker, mock_graphql_connection):
    # Arrange
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = requests.exceptions.RequestException("Network error")
    mock_requests_session_post.return_value = mock_response

    your_instance = GraphQLHook("graph_ql_conn")
    with pytest.raises(requests.exceptions.RequestException, match="Network error"):
        your_instance.get_session()


def test_run_graphql_query_success(mock_requests_session_post, mocker, mock_graphql_connection):
    # Arrange
    mock_response = Mock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"data": {"result": "success"}}
    mock_requests_session_post.return_value = mock_response

    your_instance = GraphQLHook("graph_ql_conn")

    # Act
    result = your_instance.run_graphql_query(graphql_query="your_query")

    # Assert
    assert result == {"data": {"result": "success"}}


def test_run_graphql_query_failure(mock_requests_session_post, mocker, mock_graphql_connection):
    # Arrange
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Mocked HTTPError")
    mock_requests_session_post.return_value = mock_response

    your_instance = GraphQLHook("graph_ql_conn")

    # Act and Assert
    with pytest.raises(requests.exceptions.HTTPError):
        your_instance.run_graphql_query(graphql_query="your_query")


def test_run_graphql_query_with_variables(mock_requests_session_post, mocker, mock_graphql_connection):
    # Arrange
    mock_response = Mock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"data": {"result": "success"}}
    mock_requests_session_post.return_value = mock_response

    your_instance = GraphQLHook("graph_ql_conn")

    variables = {"variable1": "value1", "variable2": "value2"}

    # Act
    result = your_instance.run_graphql_query(graphql_query="your_query", variables=variables)

    # Assert
    assert result == {"data": {"result": "success"}}


@pytest.fixture
def mock_run_graphql_query(mocker):
    return mocker.patch.object(GraphQLHook, "run_graphql_query")


def test_run_graphql_paginated_query_single_page(mock_run_graphql_query, mocker, mock_graphql_connection):
    # Arrange
    mock_response = {"data": {"result": "success"}, "pageInfo": {"hasNextPage": False, "endCursor": "null"}}
    mock_run_graphql_query.return_value = mock_response

    your_instance = GraphQLHook("graph_ql_conn")

    # Act
    result_generator = your_instance.run_graphql_paginated_query(paginated_query="your_query")

    # Assert
    result = list(result_generator)
    assert result == [mock_response]


def test_run_graphql_paginated_query_multiple_pages(mock_run_graphql_query, mocker, mock_graphql_connection):
    # Arrange
    mock_response_page1 = {
        "data": {"result": "success1"},
        "pageInfo": {"hasNextPage": True, "endCursor": "cursor1"},
    }
    mock_response_page2 = {
        "data": {"result": "success2"},
        "pageInfo": {"hasNextPage": False, "endCursor": "null"},
    }

    mock_run_graphql_query.side_effect = [mock_response_page1, mock_response_page2]

    your_instance = GraphQLHook("graph_ql_conn")

    # Act
    result_generator = your_instance.run_graphql_paginated_query(paginated_query="your_query")

    # Assert
    result = list(result_generator)
    assert result == [mock_response_page2, mock_response_page1]


def test_run_graphql_paginated_query_with_variables(mock_run_graphql_query, mocker, mock_graphql_connection):
    # Arrange
    mock_response = {"data": {"result": "success"}, "pageInfo": {"hasNextPage": False, "endCursor": "null"}}
    mock_run_graphql_query.return_value = mock_response

    your_instance = GraphQLHook("graph_ql_conn")

    variables = {"variable1": "value1", "variable2": "value2"}

    # Act
    result_generator = your_instance.run_graphql_paginated_query(
        paginated_query="your_query", variables=variables
    )

    # Assert
    result = list(result_generator)
    assert result == [mock_response]
