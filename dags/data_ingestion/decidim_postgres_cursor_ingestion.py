from datetime import timedelta

from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.python import PythonVirtualenvOperator
from airflow.utils.dates import days_ago
from airflow.datasets import Dataset

ssh_tunnel = {
    "ssh_host": Variable.get("decidim_ssh_host"),
    "ssh_port": int(Variable.get("decidim_ssh_port")),
    "ssh_user": Variable.get("decidim_ssh_user"),
    "ssh_password": Variable.get("decidim_ssh_password"),
}

origin_db_conn = {
    "pg_host": Variable.get("decidim_pg_host"),
    "pg_port": int(Variable.get("decidim_pg_port")),
    "pg_user": Variable.get("decidim_pg_user"),
    "pg_password": Variable.get("decidim_pg_password"),
    "pg_db": Variable.get("decidim_pg_db"),
}

destination_db_conn = {
    "pg_host": Variable.get("bp_dw_pg_host"),
    "pg_port": int(Variable.get("bp_dw_pg_port")),
    "pg_user": Variable.get("bp_dw_pg_user"),
    "pg_password": Variable.get("bp_dw_pg_password"),
    "pg_db": Variable.get("bp_dw_pg_db"),
}

default_args = {"owner": "data", "retries": 2, "retry_delay": timedelta(minutes=10)}

origin_schema = "public"
destination_schema = "raw"

extractions = {
    # Full Refresh Tables
    "decidim_area_types": {
        "extraction_schema": origin_schema,
        "ingestion_type": "full_refresh",
        "destination_schema": destination_schema,
    },
    "decidim_scope_types": {
        "extraction_schema": origin_schema,
        "ingestion_type": "full_refresh",
        "destination_schema": destination_schema,
    },
    "decidim_categories": {
        "extraction_schema": origin_schema,
        "ingestion_type": "full_refresh",
        "destination_schema": destination_schema,
    },
    # Incremental Tables
    "decidim_components": {
        "extraction_schema": origin_schema,
        "ingestion_type": "incremental",
        "incremental_filter": "updated_at >= '{{ ds }}' and updated_at < '{{ macros.ds_add(ds, 1) }}'",
        "destination_schema": destination_schema,
    },
    "decidim_participatory_processes": {
        "extraction_schema": origin_schema,
        "ingestion_type": "incremental",
        "incremental_filter": "updated_at >= '{{ ds }}' and updated_at < '{{ macros.ds_add(ds, 1) }}'",
        "destination_schema": destination_schema,
    },
    "decidim_participatory_process_types": {
        "extraction_schema": origin_schema,
        "ingestion_type": "incremental",
        "incremental_filter": "updated_at >= '{{ ds }}' and updated_at < '{{ macros.ds_add(ds, 1) }}'",
        "destination_schema": destination_schema,
    },
    "decidim_areas": {
        "extraction_schema": origin_schema,
        "ingestion_type": "incremental",
        "incremental_filter": "updated_at >= '{{ ds }}' and updated_at < '{{ macros.ds_add(ds, 1) }}'",
        "destination_schema": destination_schema,
    },
    "decidim_users": {
        "extraction_schema": origin_schema,
        "ingestion_type": "incremental",
        "incremental_filter": "updated_at >= '{{ ds }}' and updated_at < '{{ macros.ds_add(ds, 1) }}'",
        "destination_schema": destination_schema,
    },
    "decidim_proposals_proposals": {
        "extraction_schema": origin_schema,
        "ingestion_type": "incremental",
        "incremental_filter": "updated_at >= '{{ ds }}' and updated_at < '{{ macros.ds_add(ds, 1) }}'",
        "destination_schema": destination_schema,
    },
    "decidim_scopes": {
        "extraction_schema": origin_schema,
        "ingestion_type": "incremental",
        "incremental_filter": "updated_at >= '{{ ds }}' and updated_at < '{{ macros.ds_add(ds, 1) }}'",
        "destination_schema": destination_schema,
    },
    "decidim_proposals_proposal_votes": {
        "extraction_schema": origin_schema,
        "ingestion_type": "incremental",
        "incremental_filter": "updated_at >= '{{ ds }}' and updated_at < '{{ macros.ds_add(ds, 1) }}'",
        "destination_schema": destination_schema,
    },
    "decidim_comments_comment_votes": {
        "extraction_schema": origin_schema,
        "ingestion_type": "incremental",
        "incremental_filter": "updated_at >= '{{ ds }}' and updated_at < '{{ macros.ds_add(ds, 1) }}'",
        "destination_schema": destination_schema,
    },
    "decidim_comments_comments": {
        "extraction_schema": origin_schema,
        "ingestion_type": "incremental",
        "incremental_filter": "updated_at >= '{{ ds }}' and updated_at < '{{ macros.ds_add(ds, 1) }}'",
        "destination_schema": destination_schema,
    },
    "decidim_coauthorships": {
        "extraction_schema": origin_schema,
        "ingestion_type": "incremental",
        "incremental_filter": "updated_at >= '{{ ds }}' and updated_at < '{{ macros.ds_add(ds, 1) }}'",
        "destination_schema": destination_schema,
    },
    "decidim_categorizations": {
        "extraction_schema": origin_schema,
        "ingestion_type": "incremental",
        "incremental_filter": "updated_at >= '{{ ds }}' and updated_at < '{{ macros.ds_add(ds, 1) }}'",
        "destination_schema": destination_schema,
    },
}


@dag(
    dag_id="decidim_postgres_cursor_ingestion",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    start_date=days_ago(1),
    tags=["ingestion"],
    catchup=False,
    concurrency=1,
    render_template_as_native_obj=True,
)
def data_ingestion_postgres():

    def extract_data(extraction, extraction_info, db_conn, ssh_tunnel=None):

        import pandas as pd
        from sqlalchemy import create_engine
        from sshtunnel import SSHTunnelForwarder

        extraction_schema = extraction_info["extraction_schema"]

        if extraction_info["ingestion_type"] == "full_refresh":
            query = f"SELECT * FROM {extraction_schema}.{extraction}"
        elif extraction_info["ingestion_type"] == "incremental":
            incremental_filter = extraction_info["incremental_filter"]
            query = f"SELECT * FROM {extraction_schema}.{extraction} where {incremental_filter}"

        print(query)

        db_host = db_conn["pg_host"]
        db_port = db_conn["pg_port"]
        db_user = db_conn["pg_user"]
        db_pw = db_conn["pg_password"]
        db_db = db_conn["pg_db"]

        if ssh_tunnel:
            with SSHTunnelForwarder(
                (ssh_tunnel["ssh_host"], ssh_tunnel["ssh_port"]),
                ssh_username=ssh_tunnel["ssh_user"],
                ssh_password=ssh_tunnel["ssh_password"],
                remote_bind_address=(db_host, db_port),
            ) as tunnel:
                engine = create_engine(
                    f"postgresql://{db_user}:{db_pw}@127.0.0.1:{tunnel.local_bind_port}/{db_db}"
                )
                df = pd.read_sql(query, engine)
        else:
            connection_string = f"postgresql://{db_user}:{db_pw}@{db_host}:{db_port}/{db_db}"
            engine = create_engine(connection_string)
            df = pd.read_sql(query, engine)

        return df

    def write_data(df, extraction, extraction_info, db_conn):

        import json

        import pandas as pd
        from sqlalchemy import MetaData, Table, create_engine
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

        db_host = db_conn["pg_host"]
        db_port = db_conn["pg_port"]
        db_user = db_conn["pg_user"]
        db_pw = db_conn["pg_password"]
        db_db = db_conn["pg_db"]

        connection_string = f"postgresql://{db_user}:{db_pw}@{db_host}:{db_port}/{db_db}"
        engine = create_engine(connection_string)

        df = df.applymap(treat_complex_columns)

        schema = extraction_info["destination_schema"]
        insertion_method = "append"

        sess = sessionmaker(bind=engine)
        session = sess()
        metadata = MetaData(schema=schema)

        table = Table(extraction, metadata, autoload_with=engine)

        if extraction_info["ingestion_type"] == "incremental":
            ids_to_delete = [str(x) for x in df["id"].unique()]
            delete_query = table.delete().where(table.c.id.in_(ids_to_delete))

        else:
            delete_query = table.delete()

        result = session.execute(delete_query)
        session.commit()

        print(f"Rows deleted: {result.rowcount}")

        session.close()

        df.to_sql(
            name=extraction,
            con=engine,
            schema=schema,
            if_exists=insertion_method,
            index=False,
        )

        print(f"DataFrame written to {schema}.{extraction}.")

    for extraction, extraction_info in extractions.items():

        extract_data_task = PythonVirtualenvOperator(
            task_id=f"extract_data_{extraction}",
            python_callable=extract_data,
            requirements=["pandas", "sqlalchemy", "sshtunnel"],
            op_args=[extraction, extraction_info, origin_db_conn, ssh_tunnel],
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
                destination_db_conn,
            ],
            system_site_packages=True,
            outlets=[Dataset(f"bronze_{extraction}")]
        )

        extract_data_task >> write_data_task


data_ingestion_postgres = data_ingestion_postgres()
