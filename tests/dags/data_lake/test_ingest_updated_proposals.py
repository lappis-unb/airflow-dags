from unittest import mock

import pytest

from dags.data_lake.ingest_updated_proposals import (
    MINIO_BUCKET,
    MINIO_CONN,
    collect_responses,
    dict_safe_get,
    get_proposal_dic,
)


def test_collect_responses():
    """
    Test case for the collect_responses function.

    This test case mocks the S3Hook and its read_key method to simulate reading a mock response from S3.
    It sets up the necessary test data and calls the collect_responses function with the test data.
    Finally, it asserts that the mock S3Hook method was called with the correct arguments.

    """
    # Mock the S3Hook and its read_key method
    with mock.patch("dags.data_lake.ingest_updated_proposals.S3Hook") as mock_s3_hook:
        # Set up the mock response
        mock_response = '{"id": 1, "name": "Proposal 1"}'
        mock_s3_hook.return_value.read_key.return_value = mock_response

        # Set up the test data
        ids = ["1", "2", "3"]
        zone = "zone1"
        ds_nodash = "20220101"
        suffix = "json"

        # Call the function to collect responses
        collect_responses(ids, zone, ds_nodash, suffix)

        # Assert the mock S3Hook method was called with the correct arguments
        mock_s3_hook.assert_called_once_with(MINIO_CONN)
        mock_s3_hook.return_value.read_key.assert_any_call(
            f"updated_proposals/{zone}/{ds_nodash}_1.{suffix}", MINIO_BUCKET
        )


@pytest.mark.parametrize(
    "_dict, key, expected_result",
    [
        ({"key": "value"}, "key", "value"),  # Test case 1: Key exists and value is not None
        ({"key": None}, "key", {}),  # Test case 2: Key exists and value is None
        ({}, "key", {}),  # Test case 3: Key does not exist
    ],
)
def test_dict_safe_get(_dict, key, expected_result):
    """
    Test the dict_safe_get function.

    Args:
    ----
        _dict (dict): The dictionary to retrieve the value from.
        key (str): The key to look for in the dictionary.
        expected_result (any): The expected result when retrieving the value.

    Returns:
    -------
        None: This function does not return anything.

    Raises:
    ------
        AssertionError: If the retrieved value does not match the expected result.
    """
    assert dict_safe_get(_dict, key) == expected_result


def test_get_proposal_dic():
    """
    Test case for the get_proposal_dic function.

    This test case verifies that the get_proposal_dic function
    correctly transforms a proposal into a dictionary with the expected key-value pairs.

    The function sets up the test data, calls the get_proposal_dic
    function with the test data, and asserts that the returned dictionary
    matches the expected dictionary.

    Returns:
    -------
        None
    """
    # Set up the test data
    extract_text = lambda translations: translations[0]["text"] if translations else None
    main_title = "Main Title"
    component_id = "123"
    component_name = "Component Name"
    participatory_space_id = "789"
    proposal = {
        "id": "456",
        "createdAt": "2022-01-01T10:00:00",
        "participatory_space_id": participatory_space_id,
        "updatedAt": "2022-01-02T12:00:00",
        "author": {"name": "John Doe", "nickname": "johndoe", "organizationName": "ACME Corp"},
        "body": {"translations": [{"text": "Proposal Body"}]},
        "category": {"name": {"translations": [{"text": "Category Name"}]}},
        "title": {"translations": [{"text": "Proposal Title"}]},
        "authorsCount": 2,
        "userAllowedToComment": True,
        "endorsementsCount": 10,
        "totalCommentsCount": 5,
        "versionsCount": 3,
        "voteCount": 20,
        "commentsHaveAlignment": True,
        "commentsHaveVotes": True,
        "createdInMeeting": True,
        "hasComments": True,
        "official": True,
        "fingerprint": {"value": "abc123"},
        "position": "1",
        "reference": "REF-123",
        "scope": "Global",
        "state": "Open",
    }

    # Call the function to get the proposal dictionary
    proposal_data = get_proposal_dic(
        extract_text, main_title, component_id, component_name, proposal, participatory_space_id
    )

    # Assert the expected values
    assert proposal_data == {
        "main_title": "Main Title",
        "participatory_space_id": "789",
        "component_id": "123",
        "component_name": "Component Name",
        "proposal_id": "456",
        "proposal_created_at": "2022-01-01T10:00:00",
        "proposal_published_at": None,
        "proposal_updated_at": "2022-01-02T12:00:00",
        "author_name": "John Doe",
        "author_nickname": "johndoe",
        "author_organization": "ACME Corp",
        "proposal_body": "Proposal Body",
        "category_name": "Category Name",
        "proposal_title": "Proposal Title",
        "authors_count": 2,
        "user_allowed_to_comment": True,
        "endorsements_count": 10,
        "total_comments_count": 5,
        "versions_count": 3,
        "vote_count": 20,
        "comments_have_alignment": True,
        "comments_have_votes": True,
        "created_in_meeting": True,
        "has_comments": True,
        "official": True,
        "fingerprint": "abc123",
        "position": "1",
        "reference": "REF-123",
        "scope": "Global",
        "state": "Open",
    }
