import json
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import macros
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.hooks.base_hook import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import MetaData, Table
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.orm import sessionmaker

default_args = {"owner": "data", "retries": 0, "retry_delay": timedelta(minutes=10)}

limit = 100


POSTGRES_CONN_ID = "pg_bp_analytics"
SCHEMA = "raw"
TABLE_NAME = "matomo_detailed_visits"
dataset = Dataset("bronze_matomo_detailed_visits")


@dag(
    dag_id="matomo_detailed_visits_ingestion",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    start_date=datetime(2023, 6, 1),
    tags=["ingestion"],
    catchup=True,
    concurrency=1,
    max_active_runs=1,
    render_template_as_native_obj=True,
)
def data_ingestion_matomo_detailed_visits():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task(provide_context=True)
    def fetch_visits_details(limit=100, **context):
        """
        Fetches visitor details from Matomo Live! API within a specified date range, paginated.

        Parameters:
        ----------
            api_url (str): Base URL of the Matomo instance.
            site_id (str): The ID of the website to retrieve data for.
            api_token (str): API authentication token.
            start_date (str): Start date in 'YYYY-MM-DD' format.
            end_date (str): End date in 'YYYY-MM-DD' format.
            limit (int): Number of records to fetch per page (default is 100).

        Returns:
        -------
            list: A list of all visit details in JSON format.
        """
        # Configs
        site_id, matomo_url, api_token = _get_matomo_credentials()
        start_date = context["ds"]
        end_date = macros.ds_add(context["ds"], 1)

        all_visits = []
        offset = 0

        while True:
            params = {
                "module": "API",
                "method": "Live.getLastVisitsDetails",
                "idSite": site_id,
                "period": "range",  # Fetch data within the date range
                "date": f"{start_date},{end_date}",
                "format": "JSON",
                "filter_limit": limit,
                "filter_offset": offset,
                "token_auth": api_token,
            }

            response = requests.get(matomo_url, params=params)

            # Check if the request was successful
            if response.status_code == 200:
                visits = response.json()
                if not visits:
                    # Break the loop if no more visits are returned
                    break
                all_visits.extend(visits)
                offset += limit  # Increase the offset for the next batch
            else:
                raise Exception(f"Error fetching data: {response.status_code} - {response.text}")

        return all_visits

    def _get_matomo_credentials():
        """
        Retorna as credenciais necessárias para se conectar ao Matomo.

        Returns:
        -------
            tuple: Uma tupla contendo as seguintes informações:
                - site_id (str): O ID do site no Matomo.
                - matomo_url (str): A URL do Matomo.
                - api_token (str): O token de API do Matomo.
        """
        matomo_conn = BaseHook.get_connection("matomo_conn")
        matomo_url = matomo_conn.host
        api_token = matomo_conn.password
        site_id = matomo_conn.login
        return site_id, matomo_url, api_token

    @task(provide_context=True, outlets=dataset)
    def write_data(data, table=TABLE_NAME, schema=SCHEMA):

        engine = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_sqlalchemy_engine()

        if not data:
            print("No data to write!")
            return None

        df = pd.DataFrame(data)

        pd.set_option("display.max_columns", None)

        def treat_complex_columns(col_value):
            if isinstance(col_value, (dict, list)):
                return json.dumps(col_value, ensure_ascii=False)
            return col_value

        df = df.applymap(treat_complex_columns)
        try:
            delete_matching_rows_from_table(table, schema, engine, df)
        except NoSuchTableError:
            print(f"Table {schema}.{table} não existe, criando uma nova tabela.")
        df.to_sql(TABLE_NAME, con=engine, schema=SCHEMA, if_exists="append", index=False)
        print(f"DataFrame written to {schema}.{table}.")

    def delete_matching_rows_from_table(table, schema, engine, df):
        """
        Deleta as linhas da tabela especificada com base nos valores únicos da coluna 'serverDate'.

        Args:
        ----
            table (str): O nome da tabela a ser deletada.
            schema (str): O esquema da tabela.
            engine (sqlalchemy.engine.Engine): O objeto de conexão do SQLAlchemy.
            df (pandas.DataFrame): O DataFrame contendo os valores únicos da coluna 'serverDate'.

        Returns:
        -------
            None
        """
        sess = sessionmaker(bind=engine)
        session = sess()
        metadata = MetaData(schema=schema)

        table = Table(table, metadata, autoload_with=engine)

        to_delete = [str(x) for x in df["serverDate"].unique()]
        delete_query = table.delete().where(table.c.serverDate.in_(to_delete))

        result = session.execute(delete_query)
        session.commit()

        print(f"Rows deleted: {result.rowcount}")

        session.close()

    extract_data_task = fetch_visits_details(limit)
    write_data_task = write_data(extract_data_task, table=TABLE_NAME, schema=SCHEMA)
    start >> extract_data_task >> write_data_task >> end


data_ingestion_matomo_detailed_visits()
