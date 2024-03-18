import pytest
from airflow.models.connection import Connection


@pytest.fixture
def mock_get_component_type(mocker):
    return mocker.patch("plugins.components.base_component.component.ComponentBaseHook.get_component_type")


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
