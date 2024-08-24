from datetime import timedelta

from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.python import PythonVirtualenvOperator
from airflow.utils.dates import days_ago
from airflow.datasets import Dataset


destination_db_conn = {
    "pg_host": Variable.get("bp_dw_pg_host"),
    "pg_port": int(Variable.get("bp_dw_pg_port")),
    "pg_user": Variable.get("bp_dw_pg_user"),
    "pg_password": Variable.get("bp_dw_pg_password"),
    "pg_db": Variable.get("bp_dw_pg_db"),
}

default_args = {"owner": "data", "retries": 2, "retry_delay": timedelta(minutes=10)}

api_url = "https://ew.dataprev.gov.br/index.php"
site_id = "18"
api_token = Variable.get("matomo_api_token")

start_date = "{{ ds }}"
end_date = "{{ macros.ds_add(ds, 1) }}"
limit = 100


@dag(
    dag_id="matomo_detailed_visits_ingestion",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    start_date=days_ago(1),
    tags=["ingestion"],
    catchup=False,
    concurrency=1,
    render_template_as_native_obj=True,
)
def data_ingestion_matomo_detailed_visits():

    def fetch_visits_details(api_url, site_id, api_token, start_date, end_date, limit=100):
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
        import requests

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

            response = requests.get(api_url, params=params)

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

    def write_data(data, extraction, schema, db_conn):

        import json

        import pandas as pd
        from sqlalchemy import MetaData, Table, create_engine
        from sqlalchemy.orm import sessionmaker

        if not data:
            print("No data to write!")
            return None

        select_columns = [
            "idVisit",
            "visitorId",
            "serverDate",
            "serverTimestamp",
            "userId",
            "visitorType",
            "referrerType",
            "referrerTypeName",
            "referrerName",
            "referrerKeyword",
            "referrerKeywordPosition",
            "referrerUrl",
            "referrerSearchEngineUrl",
            "referrerSearchEngineIcon",
            "referrerSocialNetworkUrl",
            "referrerSocialNetworkIcon",
            "deviceType",
            "deviceBrand",
            "deviceModel",
            "operatingSystemName",
            "continent",
            "country",
            "countryCode",
            "region",
            "regionCode",
            "city",
            "actionDetails",
        ]

        df = pd.DataFrame(data)

        df = df[select_columns]

        df_exploded = df.explode("actionDetails").reset_index(drop=True)

        df_normalized = pd.json_normalize(df_exploded["actionDetails"])

        df_normalized = df_normalized[["url", "pageIdAction", "timeSpent", "timestamp"]]

        df = df_exploded.drop("actionDetails", axis=1).join(df_normalized)

        def treat_complex_columns(col_value):
            if isinstance(col_value, (dict, list)):
                return json.dumps(col_value, ensure_ascii=False)
            return col_value

        print("##############################################################")
        pd.set_option("display.max_columns", None)
        pd.set_option("display.max_colwidth", 100)
        print(df)
        print("##############################################################")

        db_host = db_conn["pg_host"]
        db_port = db_conn["pg_port"]
        db_user = db_conn["pg_user"]
        db_pw = db_conn["pg_password"]
        db_db = db_conn["pg_db"]

        connection_string = f"postgresql://{db_user}:{db_pw}@{db_host}:{db_port}/{db_db}"
        engine = create_engine(connection_string)

        df = df.applymap(treat_complex_columns)

        sess = sessionmaker(bind=engine)
        session = sess()
        metadata = MetaData(schema=schema)

        table = Table(extraction, metadata, autoload_with=engine)

        to_delete = [str(x) for x in df["serverDate"].unique()]
        delete_query = table.delete().where(table.c.serverDate.in_(to_delete))

        result = session.execute(delete_query)
        session.commit()

        print(f"Rows deleted: {result.rowcount}")

        session.close()

        df.to_sql(
            name=extraction,
            con=engine,
            schema=schema,
            if_exists="append",
            index=False,
        )

        print(f"DataFrame written to {schema}.{extraction}.")

    extract_data_task = PythonVirtualenvOperator(
        task_id="extract_data",
        python_callable=fetch_visits_details,
        requirements=[],
        op_args=[api_url, site_id, api_token, start_date, end_date, limit],
        system_site_packages=True,
    )

    write_data_task = PythonVirtualenvOperator(
        task_id="write_data",
        python_callable=write_data,
        requirements=["pandas", "sqlalchemy"],
        op_args=[
            "{{ ti.xcom_pull(task_ids='extract_data') }}",
            "matomo_detailed_visits",
            "raw",
            destination_db_conn,
        ],
        system_site_packages=True,
        outlets=[Dataset("bronze_matomo_detailed_visits")]
    )

    extract_data_task >> write_data_task


data_ingestion_matomo_detailed_visits = data_ingestion_matomo_detailed_visits()
