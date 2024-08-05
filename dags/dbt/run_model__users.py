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
    "run_model__users",
    default_args=default_args,
    schedule='@daily',
    start_date=days_ago(1),
    tags=["dbt", "model"],
) as dag:

    end_task = EmptyOperator(
        task_id="end",
        outlets=[Dataset("users_model")],
    )

    users_task = BashOperator(
        task_id='run_users',
        bash_command='dbt deps && dbt run --select users \
&& rm -r /tmp/dbt_target_run_users /tmp/dbt_logs_run_users',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
            'DBT_TARGET_PATH': '/tmp/dbt_target_run_users',
            'DBT_LOG_PATH': '/tmp/dbt_logs_run_users'
        },
        cwd='/opt/airflow/dags-config/repo/plugins/dbt_pg_project',
        append_env=True
    )

    unique_users_user_id_task = BashOperator(
        task_id='test_unique_users_user_id',
        bash_command='dbt deps && dbt test --select unique_users_user_id \
&& rm -r /tmp/dbt_target_test_unique_users_user_id /tmp/dbt_logs_test_unique_users_user_id',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
            'DBT_TARGET_PATH': '/tmp/dbt_target_test_unique_users_user_id',
            'DBT_LOG_PATH': '/tmp/dbt_logs_test_unique_users_user_id'
        },
        cwd='/opt/airflow/dags-config/repo/plugins/dbt_pg_project',
        append_env=True
    )

    not_null_users_user_id_task = BashOperator(
        task_id='test_not_null_users_user_id',
        bash_command='dbt deps && dbt test --select not_null_users_user_id \
&& rm -r /tmp/dbt_target_test_not_null_users_user_id /tmp/dbt_logs_test_not_null_users_user_id',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
            'DBT_TARGET_PATH': '/tmp/dbt_target_test_not_null_users_user_id',
            'DBT_LOG_PATH': '/tmp/dbt_logs_test_not_null_users_user_id'
        },
        cwd='/opt/airflow/dags-config/repo/plugins/dbt_pg_project',
        append_env=True
    )

    column_completeness_test_source_users_user_id__id__bronze__decidim_users_task = BashOperator(
        task_id='test_column_completeness_test_source_users_user_id__id__bronze__decidim_users',
        bash_command='dbt deps && dbt test --select column_completeness_test_source_users_user_id__id__bronze__decidim_users \
&& rm -r /tmp/dbt_target_test_column_completeness_test_source_users_user_id__id__bronze__decidim_users /tmp/dbt_logs_test_column_completeness_test_source_users_user_id__id__bronze__decidim_users',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
            'DBT_TARGET_PATH': '/tmp/dbt_target_test_column_completeness_test_source_users_user_id__id__bronze__decidim_users',
            'DBT_LOG_PATH': '/tmp/dbt_logs_test_column_completeness_test_source_users_user_id__id__bronze__decidim_users'
        },
        cwd='/opt/airflow/dags-config/repo/plugins/dbt_pg_project',
        append_env=True
    )

    referential_integrity_test_proposals_user_id__user_id__users_task = BashOperator(
        task_id='test_referential_integrity_test_proposals_user_id__user_id__users',
        bash_command='dbt deps && dbt test --select referential_integrity_test_proposals_user_id__user_id__users \
&& rm -r /tmp/dbt_target_test_referential_integrity_test_proposals_user_id__user_id__users /tmp/dbt_logs_test_referential_integrity_test_proposals_user_id__user_id__users',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
            'DBT_TARGET_PATH': '/tmp/dbt_target_test_referential_integrity_test_proposals_user_id__user_id__users',
            'DBT_LOG_PATH': '/tmp/dbt_logs_test_referential_integrity_test_proposals_user_id__user_id__users'
        },
        cwd='/opt/airflow/dags-config/repo/plugins/dbt_pg_project',
        append_env=True
    )

    referential_integrity_test_comments_user_id__user_id__users_task = BashOperator(
        task_id='test_referential_integrity_test_comments_user_id__user_id__users',
        bash_command='dbt deps && dbt test --select referential_integrity_test_comments_user_id__user_id__users \
&& rm -r /tmp/dbt_target_test_referential_integrity_test_comments_user_id__user_id__users /tmp/dbt_logs_test_referential_integrity_test_comments_user_id__user_id__users',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
            'DBT_TARGET_PATH': '/tmp/dbt_target_test_referential_integrity_test_comments_user_id__user_id__users',
            'DBT_LOG_PATH': '/tmp/dbt_logs_test_referential_integrity_test_comments_user_id__user_id__users'
        },
        cwd='/opt/airflow/dags-config/repo/plugins/dbt_pg_project',
        append_env=True
    )

    referential_integrity_test_votes_user_id__user_id__users_task = BashOperator(
        task_id='test_referential_integrity_test_votes_user_id__user_id__users',
        bash_command='dbt deps && dbt test --select referential_integrity_test_votes_user_id__user_id__users \
&& rm -r /tmp/dbt_target_test_referential_integrity_test_votes_user_id__user_id__users /tmp/dbt_logs_test_referential_integrity_test_votes_user_id__user_id__users',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
            'DBT_TARGET_PATH': '/tmp/dbt_target_test_referential_integrity_test_votes_user_id__user_id__users',
            'DBT_LOG_PATH': '/tmp/dbt_logs_test_referential_integrity_test_votes_user_id__user_id__users'
        },
        cwd='/opt/airflow/dags-config/repo/plugins/dbt_pg_project',
        append_env=True
    )

    users_task >> unique_users_user_id_task >> end_task
    users_task >> not_null_users_user_id_task >> end_task
    users_task >> column_completeness_test_source_users_user_id__id__bronze__decidim_users_task >> end_task
    users_task >> referential_integrity_test_proposals_user_id__user_id__users_task >> end_task
    users_task >> referential_integrity_test_comments_user_id__user_id__users_task >> end_task
    users_task >> referential_integrity_test_votes_user_id__user_id__users_task >> end_task
