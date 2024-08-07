# fmt: off
# ruff: noqa

from airflow import DAG
from airflow.datasets import Dataset
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'DBT-Genrated',
}

with DAG(
    "run_model__api_participatory_processes",
    default_args=default_args,
    schedule=[Dataset('updated_proposals')],
    start_date=days_ago(1),
    tags=["dbt", "model"],
) as dag:

    end_task = EmptyOperator(
        task_id="end",
        outlets=[Dataset("api_participatory_processes_model")],
    )

    api_participatory_processes_task = BashOperator(
        task_id='run_api_participatory_processes',
        bash_command='rm -r /tmp/dbt_run_api_participatory_processes || true \
&& cp -r /opt/airflow/dags-config/repo/plugins/dbt_pg_project /tmp/dbt_run_api_participatory_processes \
&& cd /tmp/dbt_run_api_participatory_processes \
&& dbt deps && dbt run --select api_participatory_processes \
&& rm -r /tmp/dbt_run_api_participatory_processes',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
        },
        append_env=True
    )

    api_participatory_processes_task >> end_task
