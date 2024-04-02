import pandas as pd
from datetime import datetime
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from etl_proposals import (add_temporal_columns, 
                           dict_safe_get, 
                           _convert_dtype,
                           _verify_bucket,
                           MINIO_CONN_ID
                           )
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from unittest import mock


def test_add_temporal_columns():
    # Create a sample DataFrame
    df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
    execution_date = datetime(2022, 1, 1)

    # Call the function
    result = add_temporal_columns(df, execution_date)

    # Assert the expected values
    assert result['event_day_id'].tolist() == [20220101, 20220101, 20220101]
    assert result['available_day_id'].tolist() == [20220102, 20220102, 20220102]
    assert result['available_month_id'].tolist() == [202201, 202201, 202201]
    assert result['available_year_id'].tolist() == [2022, 2022, 2022]


def test_dict_safe_get():
    # Test case 1: Key exists in the dictionary
    _dict = {'key1': 'value1', 'key2': 'value2'}
    key = 'key1'
    expected_result = 'value1'
    assert dict_safe_get(_dict, key) == expected_result

    # Test case 2: Key does not exist in the dictionary
    _dict = {'key1': 'value1', 'key2': 'value2'}
    key = 'key3'
    expected_result = {}
    assert dict_safe_get(_dict, key) == expected_result

    # Test case 3: Value associated with the key is None
    _dict = {'key1': None, 'key2': 'value2'}
    key = 'key1'
    expected_result = {}
    assert dict_safe_get(_dict, key) == expected_result

    # Test case 4: Empty dictionary
    _dict = {}
    key = 'key1'
    expected_result = {}
    assert dict_safe_get(_dict, key) == expected_result


def test__convert_dtype():
    # Create a sample DataFrame
    df = pd.DataFrame({
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
        "proposal_createdAt": [pd.Timestamp("2022-01-01"), pd.Timestamp("2022-01-02")],
        "proposal_id": [10001, 10002],
        "proposal_publishedAt": [pd.Timestamp("2022-01-03"), pd.Timestamp("2022-01-04")],
        "proposal_title": ["Proposal 1", "Proposal 2"],
        "proposal_updatedAt": [pd.Timestamp("2022-01-05"), pd.Timestamp("2022-01-06")],
        "reference": ["Ref 1", "Ref 2"],
        "scope": ["Scope 1", "Scope 2"],
        "state": ["State 1", "State 2"],
        "totalCommentsCount": [5, 10],
        "userAllowedToComment": [True, False],
        "versionsCount": [3, 4],
        "voteCount": [100, 200]
    })

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


test__convert_dtype()
test_dict_safe_get()
test_add_temporal_columns()
import pandas as pd
from datetime import datetime
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from etl_proposals import (add_temporal_columns, 
                           dict_safe_get, 
                           _convert_dtype,
                           _verify_bucket,
                           MINIO_CONN_ID
                           )
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from unittest import mock


def test_add_temporal_columns():
    # Create a sample DataFrame
    df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
    execution_date = datetime(2022, 1, 1)

    # Call the function
    result = add_temporal_columns(df, execution_date)

    # Assert the expected values
    assert result['event_day_id'].tolist() == [20220101, 20220101, 20220101]
    assert result['available_day_id'].tolist() == [20220102, 20220102, 20220102]
    assert result['available_month_id'].tolist() == [202201, 202201, 202201]
    assert result['available_year_id'].tolist() == [2022, 2022, 2022]


def test_dict_safe_get():
    # Test case 1: Key exists in the dictionary
    _dict = {'key1': 'value1', 'key2': 'value2'}
    key = 'key1'
    expected_result = 'value1'
    assert dict_safe_get(_dict, key) == expected_result

    # Test case 2: Key does not exist in the dictionary
    _dict = {'key1': 'value1', 'key2': 'value2'}
    key = 'key3'
    expected_result = {}
    assert dict_safe_get(_dict, key) == expected_result

    # Test case 3: Value associated with the key is None
    _dict = {'key1': None, 'key2': 'value2'}
    key = 'key1'
    expected_result = {}
    assert dict_safe_get(_dict, key) == expected_result

    # Test case 4: Empty dictionary
    _dict = {}
    key = 'key1'
    expected_result = {}
    assert dict_safe_get(_dict, key) == expected_result


def test__convert_dtype():
    # Create a sample DataFrame
    df = pd.DataFrame({
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
        "proposal_createdAt": [pd.Timestamp("2022-01-01"), pd.Timestamp("2022-01-02")],
        "proposal_id": [10001, 10002],
        "proposal_publishedAt": [pd.Timestamp("2022-01-03"), pd.Timestamp("2022-01-04")],
        "proposal_title": ["Proposal 1", "Proposal 2"],
        "proposal_updatedAt": [pd.Timestamp("2022-01-05"), pd.Timestamp("2022-01-06")],
        "reference": ["Ref 1", "Ref 2"],
        "scope": ["Scope 1", "Scope 2"],
        "state": ["State 1", "State 2"],
        "totalCommentsCount": [5, 10],
        "userAllowedToComment": [True, False],
        "versionsCount": [3, 4],
        "voteCount": [100, 200]
    })

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


@mock.patch('airflow.providers.amazon.aws.hooks.s3.S3Hook.check_for_bucket')
def test__verify_bucket_true(mock_check_for_bucket):
    # Mock the return value of check_for_bucket
    mock_check_for_bucket.return_value = True

    # Create an instance of S3Hook
    hook = S3Hook(aws_conn_id=MINIO_CONN_ID,)

    # Call the function
    result = _verify_bucket(hook, bucket_name='teste')
    # Assert the expected return value
    assert result is None 

@mock.patch('airflow.providers.amazon.aws.hooks.s3.S3Hook.check_for_bucket')
def test__verify_bucket_false(mock_check_for_bucket):
    # Mock the return value of check_for_bucket
    mock_check_for_bucket.return_value = False

    # Create an instance of S3Hook
    hook = S3Hook(aws_conn_id=MINIO_CONN_ID,)

    # Call the function
    result = _verify_bucket(hook, bucket_name='teste')
    
    # Assert the expected return value
    # Replace 'expected_result' with the expected return value when check_for_bucket returns False
    assert result == "minio_tasks.create_bucket"

