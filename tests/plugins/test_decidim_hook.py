import pytest
from airflow.models.connection import Connection

from plugins.components.proposals import ProposalsHook
from plugins.decidim_hook import ComponentNotSupportedError, DecidimHook


@pytest.fixture
def mock_get_component_type(mocker):
    return mocker.patch("plugins.components.base_component.component.ComponentBaseHook.get_component_type")


@pytest.fixture
def mock_connection(mocker):
    mock_connection = Connection(
        conn_type="http",
        login="lappis",
        password="lappis_graphql",
        host="https://lab-decide.dataprev.gov.br/",
        port=80,
    )

    mock_connection_uri = mock_connection.get_uri()
    mocker.patch.dict("os.environ", AIRFLOW_CONN_DECIDIM_API=mock_connection_uri)


def test_create_proposals_hook(mock_get_component_type, mocker, mock_connection):
    # Arrange
    mock_get_component_type.return_value = "Proposals"

    conn_id = "decidim_api"
    component_id = 123
    # Act

    hook_instance = DecidimHook(conn_id, component_id)

    # Assert
    assert isinstance(hook_instance, ProposalsHook)
    assert mock_get_component_type.call_count == 3


def test_create_unsupported_hook(mock_get_component_type, mocker, mock_connection):
    # Arrange
    mock_get_component_type.return_value = "UnsupportedComponentType"

    conn_id = "decidim_api"
    component_id = 456

    # Act and Assert
    with pytest.raises(ComponentNotSupportedError):
        DecidimHook(conn_id, component_id)
    assert mock_get_component_type.call_count == 2
