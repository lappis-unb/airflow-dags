import pytest
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


def test_read_graphql_query_from_file():
    pass
