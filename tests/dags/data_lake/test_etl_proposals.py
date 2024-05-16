import io
from datetime import datetime
from unittest import mock

import pandas as pd
import pytest
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from dags.data_lake.etl_proposals import (
    LANDING_ZONE_FILE_NAME,
    MINIO_BUCKET,
    MINIO_CONN_ID,
    PRIMARY_KEY,
    PROCESSED_FILE_NAME,
    PROCESSING_FILE_NAME,
    SCHEMA,
    TABLE_NAME,
    _check_and_create_table,
    _check_empty_file,
    _convert_dtype,
    _delete_landing_zone_file,
    _get_df_save_data_postgres,
    _save_minio_processing,
    _task_extract_data,
    _task_get_ids_from_table,
    _task_move_file_s3,
    _transform_data_save_data_postgres,
    _verify_bucket,
    add_temporal_columns,
    dict_safe_get,
)


@pytest.mark.parametrize(
    """execution_date,
    expected_event_day_id,
    expected_available_day_id,
    expected_available_month_id,
    expected_available_year_id,
    expected_writing_day_id""",
    [
        (datetime(2022, 1, 1), 20220101, 20220102, 202201, 2022, int(datetime.now().strftime("%Y%m%d"))),
        (datetime(2022, 2, 15), 20220215, 20220216, 202202, 2022, int(datetime.now().strftime("%Y%m%d"))),
        (datetime(2023, 12, 31), 20231231, 20240101, 202401, 2024, int(datetime.now().strftime("%Y%m%d"))),
    ],
)
def test_add_temporal_columns(
    execution_date,
    expected_event_day_id,
    expected_available_day_id,
    expected_available_month_id,
    expected_available_year_id,
    expected_writing_day_id,
):
    # Create a sample DataFrame
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

    # Call the function
    result = add_temporal_columns(df, execution_date)

    # Assert the expected values
    assert result["event_day_id"].tolist() == [expected_event_day_id] * len(df)
    assert result["available_day_id"].tolist() == [expected_available_day_id] * len(df)
    assert result["available_month_id"].tolist() == [expected_available_month_id] * len(df)
    assert result["available_year_id"].tolist() == [expected_available_year_id] * len(df)
    assert result["writing_day_id"].tolist() == [expected_writing_day_id] * len(df)


def test_dict_safe_get():
    """
    Test the dict_safe_get function.

    This function tests the behavior of the dict_safe_get function by
    providing different test cases.

    Test case 1: Key exists in the dictionary
    Test case 2: Key does not exist in the dictionary
    Test case 3: Value associated with the key is None
    Test case 4: Empty dictionary
    """
    _dict = {"key1": "value1", "key2": "value2"}
    key = "key1"
    expected_result = "value1"
    assert dict_safe_get(_dict, key) == expected_result

    _dict = {"key1": "value1", "key2": "value2"}
    key = "key3"
    expected_result = {}
    assert dict_safe_get(_dict, key) == expected_result

    _dict = {"key1": None, "key2": "value2"}
    key = "key1"
    expected_result = {}
    assert dict_safe_get(_dict, key) == expected_result

    _dict = {}
    key = "key1"
    expected_result = {}
    assert dict_safe_get(_dict, key) == expected_result


def test__convert_dtype():
    """
    Test the _convert_dtype function to ensure correct data type conversion.

    This test function creates a sample DataFrame with various columns and data types.
    It then calls the _convert_dtype function and asserts that the resulting DataFrame
    has the expected data types for each column.

    Returns:
    -------
        None
    """
    # Create a sample DataFrame
    df = pd.DataFrame(
        {
            "author_name": ["John", "Jane"],
            "author_nickname": ["J", "J"],
            "authorsCount": [1, 2],
            "category_name": ["Category 1", "Category 2"],
            "commentsHaveAlignment": [True, False],
            "commentsHaveVotes": [False, True],
            "component_id": [1001, 1002],
            "component_name": ["Component A", "Component B"],
            "createdInMeeting": [True, False],
            "endorsementsCount": [10, 20],
            "fingerprint": ["abc123", "def456"],
            "hasComments": [True, True],
            "main_title": ["Title 1", "Title 2"],
            "official": [True, False],
            "position": ["Position 1", "Position 2"],
            "proposal_body": ["Body 1", "Body 2"],
            "proposal_createdAt": [
                pd.Timestamp("2022-01-01"),
                pd.Timestamp("2022-01-02"),
            ],
            "proposal_id": [10001, 10002],
            "proposal_publishedAt": [
                pd.Timestamp("2022-01-03"),
                pd.Timestamp("2022-01-04"),
            ],
            "proposal_title": ["Proposal 1", "Proposal 2"],
            "proposal_updatedAt": [
                pd.Timestamp("2022-01-05"),
                pd.Timestamp("2022-01-06"),
            ],
            "reference": ["Ref 1", "Ref 2"],
            "scope": ["Scope 1", "Scope 2"],
            "state": ["State 1", "State 2"],
            "totalCommentsCount": [5, 10],
            "userAllowedToComment": [True, False],
            "versionsCount": [3, 4],
            "voteCount": [100, 200],
        }
    )

    # Call the function
    result = _convert_dtype(df)

    # Assert the expected data types
    assert result.dtypes["author_name"] == "object"
    assert result.dtypes["author_nickname"] == "object"
    assert result.dtypes["authorsCount"] == "int64"
    assert result.dtypes["category_name"] == "object"
    assert result.dtypes["commentsHaveAlignment"] == "bool"
    assert result.dtypes["commentsHaveVotes"] == "bool"
    assert result.dtypes["component_id"] == "int64"
    assert result.dtypes["component_name"] == "object"
    assert result.dtypes["createdInMeeting"] == "bool"
    assert result.dtypes["endorsementsCount"] == "int64"
    assert result.dtypes["fingerprint"] == "object"
    assert result.dtypes["hasComments"] == "bool"
    assert result.dtypes["main_title"] == "object"
    assert result.dtypes["official"] == "bool"
    assert result.dtypes["position"] == "object"
    assert result.dtypes["proposal_body"] == "object"
    assert result.dtypes["proposal_createdAt"] == "datetime64[ns]"
    assert result.dtypes["proposal_id"] == "int64"
    assert result.dtypes["proposal_publishedAt"] == "datetime64[ns]"
    assert result.dtypes["proposal_title"] == "object"
    assert result.dtypes["proposal_updatedAt"] == "datetime64[ns]"
    assert result.dtypes["reference"] == "object"
    assert result.dtypes["scope"] == "object"
    assert result.dtypes["state"] == "object"
    assert result.dtypes["totalCommentsCount"] == "int64"
    assert result.dtypes["userAllowedToComment"] == "bool"
    assert result.dtypes["versionsCount"] == "int64"
    assert result.dtypes["voteCount"] == "int64"


@mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.check_for_bucket")
def test__verify_bucket_true(mock_check_for_bucket):
    """
    Test case for _verify_bucket function when the bucket exists.

    Args:
    ----
        mock_check_for_bucket (MagicMock): Mock object for S3Hook.check_for_bucket.

    Returns:
    -------
        None: This function does not return anything.
    """
    # Mock the return value of check_for_bucket
    mock_check_for_bucket.return_value = True

    # Create an instance of S3Hook
    hook = S3Hook(
        aws_conn_id=MINIO_CONN_ID,
    )

    # Call the function
    result = _verify_bucket(hook, bucket_name="teste")
    # Assert the expected return value
    assert result is None


@mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.check_for_bucket")
def test__verify_bucket_false(mock_check_for_bucket):
    """
    Test case for the _verify_bucket function when check_for_bucket returns False.

    Args:
    ----
        mock_check_for_bucket (Mock): The mocked check_for_bucket function.

    Returns:
    -------
        str: The expected return value "minio_tasks.create_bucket".
    """
    # Mock the return value of check_for_bucket
    mock_check_for_bucket.return_value = False

    # Create an instance of S3Hook
    hook = S3Hook(
        aws_conn_id=MINIO_CONN_ID,
    )

    # Call the function
    result = _verify_bucket(hook, bucket_name="teste")

    # Assert the expected return value
    # Replace 'expected_result' with the expected return value when check_for_bucket returns False
    assert result == "minio_tasks.create_bucket"


def test__task_extract_data():
    """
    Test function for _task_extract_data.

    This function tests the behavior of the _task_extract_data function by mocking the necessary dependencies
    and asserting the expected function calls.

    Args:
    ----
        None

    Returns:
    -------
        None
    """
    # Mock the context
    context = {
        "execution_date": datetime(2022, 1, 1),
    }

    # Mock the GraphQLHook and its session
    mock_session = mock.MagicMock()
    mock_session.post.return_value = mock.MagicMock(text='{"data": "sample_data"}')
    mock_hook = mock.MagicMock()
    mock_hook.get_session.return_value = mock_session

    # Mock the S3Hook
    mock_s3hook = mock.MagicMock()

    # Patch the GraphQLHook and S3Hook
    with mock.patch("dags.data_lake.etl_proposals.GraphQLHook", return_value=mock_hook), mock.patch(
        "dags.data_lake.etl_proposals.S3Hook", return_value=mock_s3hook
    ):
        # Call the function
        _task_extract_data(**context)

    # Assert the expected function calls
    mock_hook.get_session.assert_called_once()
    mock_session.post.assert_called_once_with(
        mock_hook.api_url,
        json={
            "query": mock.ANY,
            "variables": {"start_date": "2022-01-01", "end_date": "2022-01-02"},
        },
    )
    mock_s3hook.load_string.assert_called_once_with(
        string_data='{"data": "sample_data"}',
        bucket_name=mock.ANY,
        key=mock.ANY,
        replace=True,
    )


def test__save_minio_processing():
    """
    Test case for the _save_minio_processing function.

    This test case verifies that the _save_minio_processing function correctly saves a DataFrame to Minio.

    Steps:
    1. Mock the MinioClient.
    2. Create a sample DataFrame.
    3. Call the _save_minio_processing function with the sample DataFrame.
    4. Assert that the expected function calls were made to save the DataFrame to Minio.

    """
    # Mock the MinioClient
    mock_minio = mock.MagicMock()

    # Create a sample DataFrame
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

    # Call the function
    _save_minio_processing("20220101", mock_minio, df)

    # Assert the expected function calls
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    mock_minio.load_string.assert_called_once_with(
        string_data=csv_buffer.getvalue(),
        bucket_name=MINIO_BUCKET,
        key=PROCESSING_FILE_NAME.format(date_file="20220101"),
        replace=True,
    )


def test__delete_landing_zone_file():
    """
    Test case for the _delete_landing_zone_file function.

    This function tests the behavior of the _delete_landing_zone_file function by mocking the context,
    the S3Hook, and patching the S3Hook. It then calls the _delete_landing_zone_file function and
    asserts the expected function calls.

    The _delete_landing_zone_file function is responsible for deleting a file from the landing zone
    in an S3 bucket. It takes a context object as input, which should contain the execution date.

    Returns:
    -------
        None
    """
    # Mock the context
    context = {
        "execution_date": datetime(2022, 1, 1),
    }

    # Mock the S3Hook
    mock_s3hook = mock.MagicMock()

    # Patch the S3Hook
    with mock.patch("dags.data_lake.etl_proposals.S3Hook", return_value=mock_s3hook):
        # Call the function
        _delete_landing_zone_file(context)

    # Assert the expected function calls
    date_file = context["execution_date"].strftime("%Y%m%d")
    mock_s3hook.delete_objects.assert_called_once_with(
        bucket=MINIO_BUCKET,
        keys=LANDING_ZONE_FILE_NAME.format(date_file=date_file),
    )


def test__check_empty_file():
    """
    Test case for the _check_empty_file function.

    This function tests the behavior of the _check_empty_file function by mocking the context,
    the S3Hook, and patching the S3Hook. It asserts the expected function calls and return value.

    Returns:
    -------
        None
    """
    # Mock the context
    context = {
        "execution_date": datetime(2022, 1, 1),
    }

    # Mock the S3Hook
    mock_s3hook = mock.MagicMock()
    mock_s3hook.read_key.return_value = ""

    # Patch the S3Hook
    with mock.patch("dags.data_lake.etl_proposals.S3Hook", return_value=mock_s3hook):
        # Call the function
        result = _check_empty_file(**context)

    # Assert the expected function calls and return value
    date_file = context["execution_date"].strftime("%Y%m%d")
    mock_s3hook.read_key.assert_called_once_with(
        key=PROCESSING_FILE_NAME.format(date_file=date_file),
        bucket_name=MINIO_BUCKET,
    )
    assert result == "load.empty_file"


def test__check_empty_file_non_empty():
    """
    Test case for the _check_empty_file function when the file is non-empty.

    This test case mocks the context and the S3Hook, and patches the S3Hook to simulate
    reading a non-empty file from S3. It then calls the _check_empty_file function and
    asserts the expected function calls and return value.

    Returns:
    -------
        None
    """
    # Mock the context
    context = {
        "execution_date": datetime(2022, 1, 1),
    }

    # Mock the S3Hook
    mock_s3hook = mock.MagicMock()
    mock_s3hook.read_key.return_value = "some data"

    # Patch the S3Hook
    with mock.patch("dags.data_lake.etl_proposals.S3Hook", return_value=mock_s3hook):
        # Call the function
        result = _check_empty_file(**context)

    # Assert the expected function calls and return value
    date_file = context["execution_date"].strftime("%Y%m%d")
    mock_s3hook.read_key.assert_called_once_with(
        key=PROCESSING_FILE_NAME.format(date_file=date_file),
        bucket_name=MINIO_BUCKET,
    )
    assert result == "load.check_and_create_table"


def test__check_and_create_table():
    """
    Test function for checking and creating a table.

    This function tests the behavior of the _check_and_create_table function by mocking the engine,
    calling the function with different scenarios, and asserting the expected function calls.

    Returns:
    -------
        None
    """
    # Mock the engine
    mock_engine = mock.MagicMock()

    # Mock the has_table method to return False
    mock_engine.has_table.return_value = False

    # Call the function
    _check_and_create_table(mock_engine)

    # Assert the expected function calls
    mock_engine.has_table.assert_called_once_with(table_name=TABLE_NAME, schema=SCHEMA)
    mock_engine.execute.assert_has_calls(
        [
            mock.call(
                f"""
          CREATE TABLE {SCHEMA}.{TABLE_NAME} (
            main_title text NULL,
            slug text NULL,
            component_id int8 NULL,
            component_name text NULL,
            proposal_id int8 NOT NULL,
            proposal_created_at timestamp NULL,
            proposal_published_at timestamp NULL,
            proposal_updated_at timestamp NULL,
            author_name text NULL,
            author_nickname text NULL,
            author_organization text NULL,
            proposal_body text NULL,
            category_name text NULL,
            proposal_title text NULL,
            authors_count int8 NULL,
            user_allowed_to_comment bool NULL,
            endorsements_count int8 NULL,
            total_comments_count int8 NULL,
            versions_count int8 NULL,
            vote_count int8 NULL,
            comments_have_alignment bool NULL,
            comments_have_votes bool NULL,
            created_in_meeting bool NULL,
            has_comments bool NULL,
            official bool NULL,
            fingerprint text NULL,
            position int8 NULL,
            reference text NULL,
            scope text NULL,
            state text NULL,
            event_day_id int8 NULL,
            available_day_id int8 NULL,
            available_month_id int8 NULL,
            available_year_id int8 NULL,
            writing_day_id int8 NULL
          );
          """
            ),
            mock.call(f"ALTER TABLE {SCHEMA}.{TABLE_NAME} ADD PRIMARY KEY ({PRIMARY_KEY});"),
        ]
    )

    # Reset the mock calls
    mock_engine.reset_mock()

    # Mock the has_table method to return True
    mock_engine.has_table.return_value = True

    # Call the function again
    _check_and_create_table(mock_engine)

    # Assert the expected function calls
    mock_engine.has_table.assert_called_once_with(table_name=TABLE_NAME, schema=SCHEMA)
    mock_engine.execute.assert_not_called()


def test__task_get_ids_from_empty_table():
    """
    Test case for the _task_get_ids_from_table function when the table is empty.

    This test case mocks the engine and connection, and sets up a mock result of an empty SQL query.
    It then calls the _task_get_ids_from_table function and asserts that the result is an empty list.

    """
    # Mock the engine and connection
    mock_connection = mock.MagicMock()
    mock_engine = mock.MagicMock()
    mock_engine.connect.return_value = mock_connection

    # Mock the result of the SQL query
    mock_result = mock.MagicMock()
    mock_result.__iter__.return_value = []
    mock_connection.execute.return_value = mock_result

    # Call the function
    result = _task_get_ids_from_table(mock_engine)

    # Assert the expected function calls
    mock_engine.execute.assert_not_called()
    assert result == []


def test__get_df_save_data_postgres():
    """
    Test case for the _get_df_save_data_postgres function.

    This test case mocks the context and the S3Hook to simulate reading data from S3.
    It then calls the _get_df_save_data_postgres function and asserts the expected DataFrame.

    Returns:
    -------
        None
    """
    # Mock the context
    context = {
        "execution_date": datetime(2022, 1, 1),
    }

    # Mock the S3Hook
    mock_s3hook = mock.MagicMock()
    mock_s3hook.read_key.return_value = "col1,col2\n1,a\n2,b\n3,c\n"

    # Patch the S3Hook
    with mock.patch("dags.data_lake.etl_proposals.S3Hook", return_value=mock_s3hook):
        # Call the function
        result = _get_df_save_data_postgres(context)

    # Assert the expected DataFrame
    expected_df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    pd.testing.assert_frame_equal(result, expected_df)


def test__transform_data_save_data_postgres():
    """
    Test function for _transform_data_save_data_postgres.

    This function tests the transformation and saving of data to PostgreSQL
    by calling the _transform_data_save_data_postgres function with sample data.

    It creates a sample DataFrame, defines the proposal_ids to exclude,
    and calls the _transform_data_save_data_postgres function with these inputs.
    The expected result is compared with the actual result using the
    pd.testing.assert_frame_equal function.

    Returns:
    -------
        None
    """
    # Mock the context
    context = {
        "execution_date": datetime(2022, 1, 1),
    }

    # Create a sample DataFrame
    df = pd.DataFrame(
        {
            "proposal_id": [10001, 10002, 10003],
            "col1": [1, 2, 3],
            "col2": ["a", "b", "c"],
        }
    )

    # Define the proposal_ids to exclude
    proposal_ids = [10001, 10003]

    # Call the function
    result = _transform_data_save_data_postgres(proposal_ids, context, df)
    # Assert the expected DataFrame
    expected_result = pd.DataFrame(
        {
            "proposal_id": [10002],
            "col1": [2],
            "col2": ["b"],
            "event_day_id": [20220101],
            "available_day_id": [20220102],
            "available_month_id": [202201],
            "available_year_id": [2022],
            "writing_day_id": [int(datetime.now().strftime("%Y%m%d"))],
        }
    )

    result.index = range(len(result))

    pd.testing.assert_frame_equal(result, expected_result)


def test__task_move_file_s3():
    """
    Test function for _task_move_file_s3.

    This function tests the behavior of the _task_move_file_s3 function by mocking the context,
    the S3Hook, and patching the S3Hook. It then calls the function and asserts the expected
    function calls made to the mock S3Hook.

    Returns:
    -------
    None
    """
    # Mock the context
    context = {
        "execution_date": datetime(2022, 1, 1),
    }

    # Mock the S3Hook
    mock_s3hook = mock.MagicMock()

    # Patch the S3Hook
    with mock.patch("dags.data_lake.etl_proposals.S3Hook", return_value=mock_s3hook):
        # Call the function
        _task_move_file_s3(context)

    # Assert the expected function calls
    date_file = context["execution_date"].strftime("%Y%m%d")
    source_filename = PROCESSING_FILE_NAME.format(date_file=date_file)
    dest_filename = PROCESSED_FILE_NAME.format(date_file=date_file)
    mock_s3hook.copy_object.assert_called_once_with(
        source_bucket_key=source_filename,
        dest_bucket_key=dest_filename,
        source_bucket_name=MINIO_BUCKET,
        dest_bucket_name=MINIO_BUCKET,
    )
    mock_s3hook.delete_objects.assert_called_once_with(
        bucket=MINIO_BUCKET,
        keys=source_filename,
    )
