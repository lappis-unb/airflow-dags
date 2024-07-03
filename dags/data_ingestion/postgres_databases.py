from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonVirtualenvOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "data",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

config = {
    "host": Variable.get("pg_host"),
    "port": Variable.get("pg_port"),
    "user": Variable.get("pg_user"),
    "password": Variable.get("pg_password"),
    "database": "postgres",
}

publication_name = "test_publication"
slot_name = "test_slot"

dag = DAG(
    "pg_ingest_dag",
    default_args=default_args,
    description="A DAG to ingest PostgreSQL data hourly using logical replication",
    schedule_interval="@hourly",
)


def ingest_postgres_data(config, publication_name, slot_name):
    from plugins.postgres_data_ingestion.replication import start_replication

    messages = start_replication(
        config=config, publication_name=publication_name, slot_name=slot_name, timeout_seconds=5
    )

    print(messages)


ingest_task = PythonVirtualenvOperator(
    task_id="ingest_postgres_data",
    python_callable=ingest_postgres_data,
    requirements=["psycopg2-binary"],
    system_site_packages=True,
    python_version="3.11",
    op_args=[config, publication_name, slot_name],
    dag=dag,
)

ingest_task  # noqa: B018
