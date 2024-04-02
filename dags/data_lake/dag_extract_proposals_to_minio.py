import io
import json
import logging
from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
)
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.exc import ProgrammingError

from plugins.graphql.hooks.graphql_hook import GraphQLHook

# Vai ser trocado para salvar em arquivo
QUERY = """
query teste ($start_date: String!, $end_date: String!) {
  participatoryProcesses  {
    title{
      translations{
        text
      }
    }
    components{
      id
      ... on Proposals   {
        __typename
        name {
          translations {
            text
          }
        }
        proposals (filter:  {publishedSince: $start_date, publishedBefore: $end_date}  ) {
          nodes {
            id
            createdAt
            publishedAt
            updatedAt
            attachments{
              thumbnail
              type
              url
            }
            author{
              id
              name
              nickname
              organizationName
            }
            body{
              translations{
                text
              }
            }
            category{
              id
              name{
                translations{
                  text
                }
              }
            }

            authorsCount
            userAllowedToComment
            endorsementsCount
            totalCommentsCount
            versionsCount
            voteCount
            commentsHaveAlignment
            commentsHaveVotes
            createdInMeeting
            hasComments
            official
            fingerprint{
              source
              value
            }
            position
            reference
            scope{
              id
              name{
                translations{
                  text
                }
              }
            }
            state
            title{
              translations{
                text
              }
            }
          }
        }
      }
    }
  }
}


"""
DECIDIM_CONN_ID = "api_decidim"
MINIO_CONN_ID = "minio_conn_id_test"
MINIO_BUCKET = "brasil-participativo-daily-csv"
COMPONENT_TYPE_TO_EXTRACT = "Proposals"
TABLE_NAME = "proposals"
SCHEMA = "raw"
PRIMARY_KEY = 'proposal_id'
RETRIES = 0
LANDING_ZONE_FILE_NAME = "landing_zone/proposals{date_file}.json"
PROCESSING_FILE_NAME = "processing/proposals{date_file}.csv"
PROCESSED_FILE_NAME = "processed/proposals{date_file}.csv"
POSTGRES_CONN_ID = "conn_postgres"


def flatten_structure_with_additional_fields(data):
    """
    Flattens the nested structure of the input data and.

    extracts additional fields for each proposal.

    Args:
    ----
      data (dict): The input data containing nested structure.

    Returns:
    -------
      list: A list of dictionaries, where each dictionary
      represents a flattened proposal with additional fields.

    """
    data = data["data"]["participatoryProcesses"]

    # Function to handle the extraction of text from nested translation dictionaries
    def extract_text(translations):
        if translations and isinstance(translations, list):
            return translations[0].get("text")

    flattened_data = []
    for item in data:
        main_title = extract_text(item.get("title", {}).get("translations", []))
        for component in item.get("components", []):
            component_id = component.get("id", "")
            component_name = extract_text(component.get("name", {}).get("translations", []))
            if "proposals" in component:
                for proposal in component.get("proposals", {}).get("nodes", []):
                    proposal_data = {
                        "main_title": main_title,
                        "component_id": component_id,
                        "component_name": component_name,
                        "proposal_id": proposal["id"],
                        "proposal_createdAt": proposal["createdAt"],
                        "proposal_publishedAt": proposal.get("publishedAt"),
                        "proposal_updatedAt": proposal.get("updatedAt"),
                        "author_name": dict_safe_get(proposal, "author").get("name"),
                        "author_nickname": dict_safe_get(proposal, "author").get("nickname"),
                        "author_organization": dict_safe_get(proposal, "author").get("organizationName"),
                        "proposal_body": extract_text(proposal.get("body", {}).get("translations", [])),
                        "category_name": extract_text(
                            dict_safe_get(dict_safe_get(proposal, "category"), "name").get("translations", [])
                        ),
                        "proposal_title": extract_text(proposal.get("title", {}).get("translations", [])),
                        "authorsCount": proposal.get("authorsCount"),
                        "userAllowedToComment": proposal.get("userAllowedToComment"),
                        "endorsementsCount": proposal.get("endorsementsCount"),
                        "totalCommentsCount": proposal.get("totalCommentsCount"),
                        "versionsCount": proposal.get("versionsCount"),
                        "voteCount": proposal.get("voteCount"),
                        "commentsHaveAlignment": proposal.get("commentsHaveAlignment"),
                        "commentsHaveVotes": proposal.get("commentsHaveVotes"),
                        "createdInMeeting": proposal.get("createdInMeeting"),
                        "hasComments": proposal.get("hasComments"),
                        "official": proposal.get("official"),
                        "fingerprint": proposal.get("fingerprint", {}).get("value"),
                        "position": proposal.get("position"),
                        "reference": proposal.get("reference"),
                        "scope": proposal.get("scope"),
                        "state": proposal.get("state"),
                    }
                    flattened_data.append(proposal_data)
    return flattened_data


def add_temporal_columns(df: pd.DataFrame, execution_date: datetime) -> pd.DataFrame:
    """
    Adds temporal columns to the DataFrame based on the execution date.

    Args:
    ----
        df (pd.DataFrame): The original DataFrame without temporal columns.
        execution_date (datetime): The execution date to base the temporal columns on.

    Returns:
    -------
        pd.DataFrame: The DataFrame with added temporal columns.
    """
    event_day_id = int(execution_date.strftime("%Y%m%d"))
    available_day = execution_date + timedelta(days=1)
    available_day_id = int(available_day.strftime("%Y%m%d"))
    available_month_id = int(available_day.strftime("%Y%m"))
    available_year_id = int(available_day.strftime("%Y"))
    writing_day_id = int(datetime.now().strftime("%Y%m%d"))

    # Add the temporal columns to the DataFrame
    df["event_day_id"] = event_day_id
    df["available_day_id"] = available_day_id
    df["available_month_id"] = available_month_id
    df["available_year_id"] = available_year_id
    df["writing_day_id"] = writing_day_id

    return df


def dict_safe_get(_dict: dict, key: str):
    """
    Retorna o valor associado à chave especificada em um dicionário.

    Se a chave não existir ou o valor for None, retorna um dicionário vazio.

    Args:
    ----
      _dict (dict): O dicionário de onde obter o valor.
      key (str): A chave do valor desejado.

    Returns:
    -------
      O valor associado à chave especificada, ou um
      dicionário vazio se a chave não existir ou o valor for None.
    """
    value = _dict.get(key)
    if not value:
        value = {}
    return value


def _check_and_create_schema(engine, schema):
    """
    Check if a schema exists in the database, if not, create it.

    Args:
    ----
    engine (sqlalchemy.engine.Engine): The SQLAlchemy engine instance.
    schema (str): The schema name.
    """
    with engine.connect() as connection:
        result = connection.execute(
            f"""SELECT EXISTS(SELECT 1 FROM
             information_schema.schemata
             WHERE schema_name = '{schema}');"""
        )
        exists = result.scalar()
        if not exists:
            connection.execute(f"CREATE SCHEMA {schema};")
            logging.info("Schema %s created successfully.", schema)


def _convert_dtype(df: pd.DataFrame) -> pd.DataFrame:
    dtypes = {
        "author_name": "str",
        "author_nickname": "str",
        "author_organization": "str",
        "authorsCount": "int",
        "category_name": "str",
        "commentsHaveAlignment": "bool",
        "commentsHaveVotes": "bool",
        "component_id": "int",
        "component_name": "str",
        "createdInMeeting": "bool",
        "endorsementsCount": "int",
        "fingerprint": "str",
        "hasComments": "bool",
        "main_title": "str",
        "official": "bool",
        "position": "str",
        "proposal_body": "str",
        "proposal_createdAt": "datetime64[ns]",
        "proposal_id": "int",
        "proposal_publishedAt": "datetime64[ns]",
        "proposal_title": "str",
        "proposal_updatedAt": "datetime64[ns]",
        "reference": "str",
        "scope": "str",
        "state": "str",
        "totalCommentsCount": "int",
        "userAllowedToComment": "bool",
        "versionsCount": "int",
        "voteCount": "int",
    }
    # remove os dtypes que não estão no df
    dtypes = {k: v for k, v in dtypes.items() if k in df.columns}
    return df.astype(dtypes, errors="ignore")


@dag(
    default_args={
        "owner": "Amoêdo/Nitai",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    },
    schedule="0 23 * * *",
    catchup=True,
    start_date=datetime(2023, 11, 10),
    max_active_runs=10,
    description=__doc__,
    tags=["decidim", "minio"],
    dag_id="fetch_process_and_clean_proposals",
)
def fetch_process_and_clean_proposals():
    """DAG que extrai dados de propostas de um GraphQL API e os armazena em um bucket MinIO."""

    @task_group(group_id="minio_tasks")
    def minio_tasks():
        @task.branch(retries=0)
        def verify_bucket():
            """
            Verifies if the specified bucket exists in
            the MinIO server.

            Returns:
            -------
              str: The name of the task to create the bucket if it doesn't exist.
            """
            if not (S3Hook(aws_conn_id=MINIO_CONN_ID).check_for_bucket(bucket_name=MINIO_BUCKET)):
                return "minio_tasks.create_bucket"

        create_bucket = S3CreateBucketOperator(
            task_id="create_bucket", bucket_name=MINIO_BUCKET, aws_conn_id=MINIO_CONN_ID
        )
        verify_bucket() >> create_bucket

    @task(provide_context=True, retries=0, trigger_rule="none_failed")
    def extract_data(**context):
        """
        Fetches data from a GraphQL API and stores it in a MinIO bucket.

        Args:
        ----
          **context: The context dictionary containing the execution date.

        Returns:
        -------
          None
        """
        date = context["execution_date"].strftime("%Y-%m-%d")
        next_date = (context["execution_date"] + timedelta(days=1)).strftime("%Y-%m-%d")
        date_file = context["execution_date"].strftime("%Y%m%d")
        # Fetch data from GraphQL API
        hook = GraphQLHook(DECIDIM_CONN_ID)
        session = hook.get_session()
        response = session.post(
            hook.api_url,
            json={"query": QUERY, "variables": {"start_date": f"{date}", "end_date": f"{next_date}"}},
        )
        # dado = response.json()
        dado = response.text
        # Store data in MinIO bucket
        S3Hook(aws_conn_id=MINIO_CONN_ID).load_string(
            string_data=dado,
            bucket_name=MINIO_BUCKET,
            key=LANDING_ZONE_FILE_NAME.format(date_file=date_file),
            replace=True,
        )

    @task_group
    def transform():
        @task(provide_context=True, retries=0, retry_delay=timedelta(seconds=5))
        def transform_data(**context):
            """
            Transforms the data by flattening the structure and saving it as a CSV file in MinIO.

            Args:
            ----
              **context: The context dictionary containing the execution date.

            Returns:
            -------
              None
            """
            date_file = context["execution_date"].strftime("%Y%m%d")
            minio = S3Hook(aws_conn_id=MINIO_CONN_ID)
            dado = minio.read_key(
                key=LANDING_ZONE_FILE_NAME.format(date_file=date_file), bucket_name=MINIO_BUCKET
            )
            dado = json.loads(dado)
            dado = flatten_structure_with_additional_fields(dado)
            df = pd.DataFrame(dado)
            df = _convert_dtype(df)
            if len(df) > 0:
              df.columns = df.columns.str.lower()
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            minio.load_string(
                string_data=csv_buffer.getvalue(),
                bucket_name=MINIO_BUCKET,
                key=PROCESSING_FILE_NAME.format(date_file=date_file),
                replace=True,
            )

        @task(provide_context=True, retries=0)
        def delete_landing_zone_file(**context):
            """
            Deletes a file from the landing zone in MinIO.

            Args:
            ----
              context (dict): The context dictionary containing the execution date.

            Returns:
            -------
              None
            """
            date_file = context["execution_date"].strftime("%Y%m%d")
            minio = S3Hook(aws_conn_id=MINIO_CONN_ID)
            print(dir(minio))
            minio.delete_objects(bucket=MINIO_BUCKET, keys=LANDING_ZONE_FILE_NAME.format(date_file=date_file))

        transform_data() >> delete_landing_zone_file()

    @task_group(
        group_id="load",
    )
    def load():
        empty_file = EmptyOperator(task_id="empty_file")

        @task.branch(provide_context=True, retries=0)
        def check_empty_file(**context):
            """
            Checks if the file in Minio bucket is empty.

            Args:
            ----
              context (dict): The context dictionary containing execution information.

            Returns:
            -------
              str: The branch to follow based on whether the file is empty or not.
            """
            minio = S3Hook(aws_conn_id=MINIO_CONN_ID)
            date_file = context["execution_date"].strftime("%Y%m%d")
            dado = minio.read_key(
                key=PROCESSING_FILE_NAME.format(date_file=date_file), bucket_name=MINIO_BUCKET
            )
            if len(dado.strip()) == 0:
                logging.warning("No data found for %s.", date_file)
                return "load.empty_file"
            return "load.check_and_create_table"

        @task(retries=RETRIES, retry_delay=timedelta(minutes=3))
        def check_and_create_table():
            """
            Checks if the table exists in the database and creates it if it doesn't exist.

            This function uses the PostgresHook to get the SQLAlchemy engine for the Postgres database.
            It then checks if the table specified by TABLE_NAME and SCHEMA exists in the database.
            If the table doesn't exist, it creates the table with the specified columns and primary key.
            """

            engine = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_sqlalchemy_engine()
            has_table = engine.has_table(table_name=TABLE_NAME, schema=SCHEMA)

            if not has_table:
                engine.execute(f"""
                CREATE TABLE {SCHEMA}.{TABLE_NAME} (
                  main_title text NULL,
                  component_id int8 NULL,
                  component_name text NULL,
                  proposal_id int8 NOT NULL,
                  proposal_createdat text NULL,
                  proposal_publishedat text NULL,
                  proposal_updatedat text NULL,
                  author_name text NULL,
                  author_nickname text NULL,
                  author_organization text NULL,
                  proposal_body text NULL,
                  category_name text NULL,
                  proposal_title text NULL,
                  authorscount int8 NULL,
                  userallowedtocomment bool NULL,
                  endorsementscount int8 NULL,
                  totalcommentscount int8 NULL,
                  versionscount int8 NULL,
                  votecount int8 NULL,
                  commentshavealignment bool NULL,
                  commentshavevotes bool NULL,
                  createdinmeeting bool NULL,
                  hascomments bool NULL,
                  official bool NULL,
                  fingerprint text NULL,
                  "position" int8 NULL,
                  reference text NULL,
                  "scope" text NULL,
                  state text NULL,
                  event_day_id int8 NULL,
                  available_day_id int8 NULL,
                  available_month_id int8 NULL,
                  available_year_id int8 NULL,
                  writing_day_id int8 NULL
                );
                """)
                engine.execute(f"ALTER TABLE {SCHEMA}.{TABLE_NAME} ADD PRIMARY KEY ({PRIMARY_KEY});")

        @task(provide_context=True, retries=RETRIES, retry_delay=timedelta(minutes=3))
        def get_ids_from_table(**context):
            """
            Gets the proposal IDs from the 'proposals' table in the database.

            Args:
            ----
              context (dict): The context dictionary containing execution information.

            Returns:
            -------
              list: A list of proposal IDs from the table.
            """
            engine = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_sqlalchemy_engine()
            with engine.connect() as connection:
                try:
                    result = connection.execute(f"SELECT proposal_id FROM {SCHEMA}.{TABLE_NAME};")
                except ProgrammingError as error:
                    logging.warning("Table does not exist. Error: %s", error)
                    return []
                proposal_ids = [row[0] for row in result]
            return proposal_ids

        @task(provide_context=True, retries=RETRIES, retry_delay=timedelta(minutes=3))
        def save_data_potgres(proposal_ids: list, **context):
            """
            Task to save data from Minio to PostgreSQL.

            Args:
            ----
              context (dict): The context dictionary containing execution information.

            Returns:
            -------
              None
            """
            date_file = context["execution_date"].strftime("%Y%m%d")
            minio = S3Hook(aws_conn_id=MINIO_CONN_ID)
            data = minio.read_key(
                key=PROCESSING_FILE_NAME.format(date_file=date_file), bucket_name=MINIO_BUCKET
            )
            csv_file = io.StringIO(data)
            df = pd.read_csv(csv_file)
            print(df.columns)
            df = df[~df["proposal_id"].isin(proposal_ids)]
            df = add_temporal_columns(df, context["execution_date"])
            ## Configure the postgres hook and insert the data
            engine = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_sqlalchemy_engine()
            # df.to_sql(TABLE_NAME, con=engine, if_exists="append", index=False, schema=SCHEMA)

            df.to_sql(TABLE_NAME, con=engine, schema=SCHEMA, if_exists="append", index=False)


        @task(provide_context=True, trigger_rule="one_success", retries=RETRIES)
        def move_file_s3(**context):
            """
            Moves a file from the source bucket to the destination bucket in MinIO.

            Args:
            ----
              context (dict): The context dictionary containing the execution date.

            Returns:
            -------
              None
            """
            date_file = context["execution_date"].strftime("%Y%m%d")
            source_filename = PROCESSING_FILE_NAME.format(date_file=date_file)
            dest_filename = PROCESSED_FILE_NAME.format(date_file=date_file)
            s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)

            s3_hook.copy_object(
                source_bucket_key=source_filename,
                dest_bucket_key=dest_filename,
                source_bucket_name=MINIO_BUCKET,
                dest_bucket_name=MINIO_BUCKET,
            )
            s3_hook.delete_objects(bucket=MINIO_BUCKET, keys=source_filename)

        _create_table = check_and_create_table()
        _move_file_s3 = move_file_s3()
        _get_ids_from_table = get_ids_from_table()
        check_empty_file() >> [empty_file, _create_table]
        (
            _create_table
            >> _get_ids_from_table
            >> save_data_potgres(_get_ids_from_table)
            >> _move_file_s3
        )
        empty_file >> _move_file_s3

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    _extract_data = extract_data()
    start >> minio_tasks() >> _extract_data >> transform() >> load() >> end


fetch_process_and_clean_proposals()
