import pytest



import pytest
from unittest.mock import patch

from dags.data_lake.ingest_update_proposals import (
    _task_get_date_id_update_proposals,
    DECIDIM_CONN_ID
)

@patch('dags.data_lake.ingest_update_proposals.GraphQLHook')
def test__task_get_date_id_update_proposals(mock_graphql_hook):
    # Arrange
    query = "query get_date_id_update_proposals { ... }"
    expected_response = "mocked response"

    mock_session = mock_graphql_hook.return_value.get_session.return_value
    mock_session.post.return_value.text = expected_response

    # Act
    response = _task_get_date_id_update_proposals(query)

    # Assert
    assert response == expected_response
    mock_graphql_hook.assert_called_once_with(DECIDIM_CONN_ID)
    mock_graphql_hook.return_value.get_session.assert_called_once()
    mock_session.post.assert_called_once_with(
        mock_graphql_hook.return_value.api_url,
        json={"query": query},
    )