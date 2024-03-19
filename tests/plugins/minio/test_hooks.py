import pytest
from airflow.models.connection import Connection
from airflow.exceptions import AirflowNotFoundException

from plugins.minio.hooks import MinioHook
from botocore.stub import Stubber

from boto3.session import Session

# https://adamj.eu/tech/2019/04/22/testing-boto3-with-pytest-fixtures/

@pytest.fixture
def mock_connection(mocker):
    mock_connection = Connection(
        conn_type="http",
        login="lappis",
        password="lappisrocks",
        host="https://minio:9000/",
    )

    mock_connection_uri = mock_connection.get_uri()
    mocker.patch.dict("os.environ", AIRFLOW_CONN_MINIO_CONN=mock_connection_uri)

@pytest.fixture
def minio_stuber():
    with Stubber(Session.client) as stubber:
        yield stubber
        stubber.assert_no_pending_responses()

def test_success_create_minio_hook(mock_connection):
    hook = MinioHook("minio_conn")
    assert hook._host == "https://minio:9000/"
    assert hook._access_key == "lappis"
    assert hook._secret == "lappisrocks"
    assert hook._session is None


@pytest.mark.parametrize("conn_id", [(12), (None), ([])])
def test_fail_create_minio_hook(conn_id):
    with pytest.raises(AssertionError):
        MinioHook(conn_id)


def test_fail_create_minio_hook_conn_not_exists():
    with pytest.raises(AirflowNotFoundException):
        MinioHook("not_exists")


def test_minio__get_session(mock_connection, minio_stuber: Stubber):
    ...
