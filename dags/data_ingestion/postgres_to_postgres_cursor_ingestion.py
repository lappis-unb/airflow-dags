import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from airflow.datasets import Dataset
from airflow.decorators import dag
from airflow.operators.python import PythonVirtualenvOperator

if TYPE_CHECKING:
    from sshtunnel import SSHTunnelForwarder


default_args = {"owner": "data", "retries": 2, "retry_delay": timedelta(minutes=10)}

origin_schema = "public"
destination_schema = "raw"


for entry in os.scandir(Path(__file__).parent.joinpath("./cursor_ingestions")):
    if not entry.name.endswith(".json"):
        continue

    with open(entry.path) as file:
        dag_config = json.load(file)

    dag_name: str = dag_config["name"]
    extractions: dict = dag_config["extractions"]
    catchup: bool = dag_config["catchup"]
    start_date: str = datetime.strptime(dag_config["start_date"], "%Y-%m-%d")

    @dag(
        dag_id=f"{dag_name}_postgres_cursor_ingestion",
        default_args=default_args,
        schedule_interval="0 4 * * *",
        start_date=start_date,
        tags=["ingestion", dag_name],
        catchup=catchup,
        concurrency=1,
        render_template_as_native_obj=True,
    )
    def data_ingestion_postgres(origin_db_connection, destination_db_connection):
        def extract_data(extraction, extraction_info, db_conn_id, ssh_tunnel: bool = False):

            import pandas as pd
            from airflow.providers.postgres.hooks.postgres import PostgresHook
            from airflow.providers.ssh.hooks.ssh import SSHHook
            from sqlalchemy import MetaData, Table, create_engine

            ssh_conn = "ssh_tunnel_decidim"
            extraction_schema = extraction_info["extraction_schema"]
            columns_to_remove = set([f"{extraction}.{x}".lower() for x in extraction_info["exclude_columns"]])

            db = PostgresHook.get_connection(db_conn_id)
            if ssh_tunnel:
                tunnel = SSHHook(ssh_conn).get_tunnel(remote_host=db.host, remote_port=db.port)
                tunnel.start()
                tunnel: SSHTunnelForwarder
                engine = create_engine(
                    f"postgresql://{db.login}:{db.password}@127.0.0.1:{tunnel.local_bind_port}/{db.schema}"
                )
            else:
                connection_string = f"postgresql://{db.login}:{db.password}@{db.host}:{db.port}/{db.schema}"
                engine = create_engine(connection_string)

            metadata = MetaData(schema=extraction_schema)
            table = Table(extraction, metadata, autoload_with=engine)

            columns = set([str(x).lower() for x in table.columns])
            columns.difference_update(columns_to_remove)

            if extraction_info["ingestion_type"] == "full_refresh":
                query = f"SELECT {','.join(columns)} FROM {extraction_schema}.{extraction}"
            elif extraction_info["ingestion_type"] == "incremental":
                incremental_filter = extraction_info["incremental_filter"]
                query = f"SELECT {','.join(columns)} FROM {extraction_schema}.{extraction}\
                    where {incremental_filter}"

            print(query)

            df = pd.read_sql(query, engine)

            tunnel.close()
            return df

        def write_data(df, extraction, extraction_info, db_conn_id):
            import json

            import pandas as pd
            from airflow.providers.postgres.hooks.postgres import PostgresHook
            from sqlalchemy import MetaData, Table
            from sqlalchemy.exc import NoSuchTableError
            from sqlalchemy.orm import sessionmaker

            def treat_complex_columns(col_value):
                if isinstance(col_value, (dict, list)):
                    return json.dumps(col_value, ensure_ascii=False)
                return col_value

            print("##############################################################")
            pd.set_option("display.max_columns", None)
            pd.set_option("display.max_colwidth", 100)
            print(df)
            print("##############################################################")

            if df.empty:
                print("No data to write!")
                return None

            engine = PostgresHook(db_conn_id).get_sqlalchemy_engine()
            df = df.applymap(treat_complex_columns)

            schema = extraction_info["destination_schema"]
            insertion_method = "append"

            sess = sessionmaker(bind=engine)
            session = sess()
            metadata = MetaData(schema=schema)
            try:
                table = Table(extraction, metadata, autoload_with=engine)

                if extraction_info["ingestion_type"] == "incremental":
                    ids_to_delete = [str(x) for x in df["id"].unique()]
                    delete_query = table.delete().where(table.c.id.in_(ids_to_delete))
                else:
                    delete_query = table.delete()

                result = session.execute(delete_query)
                session.commit()
                print(f"Rows deleted: {result.rowcount}")

            except NoSuchTableError:
                print("Table does not exists.")

            session.close()

            df.to_sql(
                name=extraction,
                con=engine,
                schema=schema,
                if_exists=insertion_method,
                index=False,
            )

            print(f"DataFrame written to {schema}.{extraction}.")

        for extraction, extraction_info in extractions.items():  # noqa: B023
            extract_data_task = PythonVirtualenvOperator(
                task_id=f"extract_data_{extraction}",
                python_callable=extract_data,
                requirements=["pandas", "sqlalchemy", "sshtunnel"],
                op_args=[extraction, extraction_info, origin_db_connection, True],
                system_site_packages=True,
            )

            write_data_task = PythonVirtualenvOperator(
                task_id=f"write_data_{extraction}",
                python_callable=write_data,
                requirements=["pandas", "sqlalchemy"],
                op_args=[
                    f"{{{{ ti.xcom_pull(task_ids='extract_data_{extraction}') }}}}",
                    extraction,
                    extraction_info,
                    destination_db_connection,
                ],
                system_site_packages=True,
                outlets=[Dataset(f"bronze_{extraction}")],
            )

            extract_data_task >> write_data_task

    data_ingestion_postgres(dag_config["origin_db_connection"], dag_config["destination_db_connection"])
