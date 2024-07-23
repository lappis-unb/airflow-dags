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
    "run_model__first_visitors_per_day",
    default_args=default_args,
    schedule=[Dataset('visit_frequency_get')],
    start_date=days_ago(1),
    tags=["dbt", "model"],
) as dag:

    end_task = EmptyOperator(
        task_id="end",
        outlets=[Dataset("first_visitors_per_day_model")],
    )

    first_visitors_per_day_task = BashOperator(
        task_id='run_first_visitors_per_day',
        bash_command='dbt deps && dbt run --select first_visitors_per_day \
&& rm -r /tmp/dbt_target_run_first_visitors_per_day /tmp/dbt_logs_run_first_visitors_per_day',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
            'DBT_TARGET_PATH': '/tmp/dbt_target_run_first_visitors_per_day',
            'DBT_LOG_PATH': '/tmp/dbt_logs_run_first_visitors_per_day'
        },
        cwd='/opt/airflow/dags-config/repo/plugins/dbt_pg_project',
        append_env=True
    )

    first_visitors_per_day_task >> end_task
