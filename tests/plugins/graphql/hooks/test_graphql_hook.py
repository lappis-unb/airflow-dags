
import pytest
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

def test_create_grahpql_hook(mock_graphql_connection):

    hook = GraphQLHook("graph_ql_conn")
    assert hook.conn_id == "graph_ql_conn"
    assert hook.api_url == "https://lab-decide.dataprev.gov.br/"
    assert hook.auth_url == "https://lab-decide.dataprev.gov.br/api/sign_in"
    assert hook.payload == {
        "user[email]": "lappis",
        "user[password]": "lappis_graphql",
    }
