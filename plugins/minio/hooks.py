"""A class for authenticating with a GraphQL API using Apache Airflow.

This class, GraphQLHook, is designed to authenticate with a GraphQL API,
using the provided connection ID in the context of Apache Airflow.
It extends the BaseHook class and provides methods for executing GraphQL queries.
"""

from contextlib import closing
from io import StringIO

import boto3
import yaml
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection


class MinioHook(BaseHook):
    def __init__(self, conn_id: str):
        """
        Initializes the MinioHook instance.

        Args:
        ----
            conn_id (str): The connection ID used for authentication.
        """
        assert isinstance(conn_id, str), "Param type of conn_id has to be str"

        conn_values = self.get_connection(conn_id)
        assert isinstance(conn_values, Connection), "conn_values was not created correctly."

        self._host = conn_values.host
        self._access_key = conn_values.login
        self._secret = conn_values.password
        self._session = None

    def _get_session(self):
        if self._session:
            return self._session

        return boto3.client(
            "s3",
            endpoint_url=self._host,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret,
            region_name="us-east-1",
        )

    def _save_to_minio(self, bucket_name, data, filename, content_type):
        self._get_session().put_object(
            Body=data,
            Bucket=bucket_name,
            Key=filename,
            ContentType=content_type,
        )

    def save_csv_data_to_minio(self, bucket_name, data, filename):
        self._save_to_minio(data=data, bucket_name=bucket_name, filename=filename, content_type="text/csv")

    def save_yaml_data_to_minio(self, bucket_name, data, filename):
        with closing(StringIO()) as buffer:
            yaml.dump(data, buffer)
            buffer.seek(0)
            self._save_to_minio(
                data=buffer.getvalue(), bucket_name=bucket_name, filename=filename, content_type="text/yaml"
            )

    def save_custom_data_to_minio(self, bucket_name, data, filename, content_type):
        self._save_to_minio(data=data, bucket_name=bucket_name, filename=filename, content_type=content_type)

    def get_file_content(self, bucket_name, filename):
        obj = self._get_session().get_object(Bucket=bucket_name, Key=filename)
        return obj["Body"].read().decode("utf-8")

    def list_filenames_in_a_bucket(self, bucket_name):
        bucket_objects = self._get_session().list_objects(Bucket=bucket_name)

        if "Contents" not in bucket_objects:
            return None

        return [s3_object["Key"] for s3_object in bucket_objects["Contents"]]
