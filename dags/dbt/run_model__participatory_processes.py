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
    "run_model__participatory_processes",
    default_args=default_args,
    schedule='@daily',
    start_date=days_ago(1),
    tags=["dbt", "model"],
) as dag:

    end_task = EmptyOperator(
        task_id="end",
        outlets=[Dataset("participatory_processes_model")],
    )

    participatory_processes_task = BashOperator(
        task_id='run_participatory_processes',
        bash_command='rm -r /tmp/dbt_run_participatory_processes || true \
&& cp -r /opt/airflow/dags-config/repo/plugins/dbt_pg_project /tmp/dbt_run_participatory_processes \
&& cd /tmp/dbt_run_participatory_processes/dbt_pg_project \
&& dbt deps && dbt run --select participatory_processes \
&& rm -r /tmp/dbt_run_participatory_processes',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
        },
        append_env=True
    )

    unique_participatory_processes_process_id_task = BashOperator(
        task_id='test_unique_participatory_processes_process_id',
        bash_command='rm -r /tmp/dbt_test_unique_participatory_processes_process_id || true \
&& cp -r /opt/airflow/dags-config/repo/plugins/dbt_pg_project /tmp/dbt_test_unique_participatory_processes_process_id \
&& cd /tmp/dbt_test_unique_participatory_processes_process_id/dbt_pg_project \
&& dbt deps && dbt test --select unique_participatory_processes_process_id \
&& rm -r /tmp/dbt_test_unique_participatory_processes_process_id',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
        },
        append_env=True
    )

    not_null_participatory_processes_process_id_task = BashOperator(
        task_id='test_not_null_participatory_processes_process_id',
        bash_command='rm -r /tmp/dbt_test_not_null_participatory_processes_process_id || true \
&& cp -r /opt/airflow/dags-config/repo/plugins/dbt_pg_project /tmp/dbt_test_not_null_participatory_processes_process_id \
&& cd /tmp/dbt_test_not_null_participatory_processes_process_id/dbt_pg_project \
&& dbt deps && dbt test --select not_null_participatory_processes_process_id \
&& rm -r /tmp/dbt_test_not_null_participatory_processes_process_id',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
        },
        append_env=True
    )

    column_completeness_test_source_participatory_processes_process_id__id__bronze__decidim_participatory_processes_task = BashOperator(
        task_id='test_column_completeness_test_source_participatory_processes_process_id__id__bronze__decidim_participatory_processes',
        bash_command='rm -r /tmp/dbt_test_column_completeness_test_source_participatory_processes_process_id__id__bronze__decidim_participatory_processes || true \
&& cp -r /opt/airflow/dags-config/repo/plugins/dbt_pg_project /tmp/dbt_test_column_completeness_test_source_participatory_processes_process_id__id__bronze__decidim_participatory_processes \
&& cd /tmp/dbt_test_column_completeness_test_source_participatory_processes_process_id__id__bronze__decidim_participatory_processes/dbt_pg_project \
&& dbt deps && dbt test --select column_completeness_test_source_participatory_processes_process_id__id__bronze__decidim_participatory_processes \
&& rm -r /tmp/dbt_test_column_completeness_test_source_participatory_processes_process_id__id__bronze__decidim_participatory_processes',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
        },
        append_env=True
    )

    referential_integrity_test_proposals_process_id__process_id__participatory_processes_task = BashOperator(
        task_id='test_referential_integrity_test_proposals_process_id__process_id__participatory_processes',
        bash_command='rm -r /tmp/dbt_test_referential_integrity_test_proposals_process_id__process_id__participatory_processes || true \
&& cp -r /opt/airflow/dags-config/repo/plugins/dbt_pg_project /tmp/dbt_test_referential_integrity_test_proposals_process_id__process_id__participatory_processes \
&& cd /tmp/dbt_test_referential_integrity_test_proposals_process_id__process_id__participatory_processes/dbt_pg_project \
&& dbt deps && dbt test --select referential_integrity_test_proposals_process_id__process_id__participatory_processes \
&& rm -r /tmp/dbt_test_referential_integrity_test_proposals_process_id__process_id__participatory_processes',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
        },
        append_env=True
    )

    participatory_processes_task >> unique_participatory_processes_process_id_task >> end_task
    participatory_processes_task >> not_null_participatory_processes_process_id_task >> end_task
    participatory_processes_task >> column_completeness_test_source_participatory_processes_process_id__id__bronze__decidim_participatory_processes_task >> end_task
    participatory_processes_task >> referential_integrity_test_proposals_process_id__process_id__participatory_processes_task >> end_task
