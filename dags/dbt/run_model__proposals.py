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
    "run_model__proposals",
    default_args=default_args,
    schedule='@daily',
    start_date=days_ago(1),
    tags=["dbt", "model"],
) as dag:

    end_task = EmptyOperator(
        task_id="end",
        outlets=[Dataset("proposals_model")],
    )

    proposals_task = BashOperator(
        task_id='run_proposals',
        bash_command='rm -r /tmp/dbt_run_proposals || true \
&& cp -r /opt/airflow/dags-config/repo/plugins/dbt_pg_project /tmp/dbt_run_proposals \
&& cd /tmp/dbt_run_proposals \
&& dbt deps && dbt run --select proposals \
&& rm -r /tmp/dbt_run_proposals',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
        },
        append_env=True
    )

    unique_proposals_proposal_id_task = BashOperator(
        task_id='test_unique_proposals_proposal_id',
        bash_command='rm -r /tmp/dbt_test_unique_proposals_proposal_id || true \
&& cp -r /opt/airflow/dags-config/repo/plugins/dbt_pg_project /tmp/dbt_test_unique_proposals_proposal_id \
&& cd /tmp/dbt_test_unique_proposals_proposal_id \
&& dbt deps && dbt test --select unique_proposals_proposal_id \
&& rm -r /tmp/dbt_test_unique_proposals_proposal_id',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
        },
        append_env=True
    )

    not_null_proposals_proposal_id_task = BashOperator(
        task_id='test_not_null_proposals_proposal_id',
        bash_command='rm -r /tmp/dbt_test_not_null_proposals_proposal_id || true \
&& cp -r /opt/airflow/dags-config/repo/plugins/dbt_pg_project /tmp/dbt_test_not_null_proposals_proposal_id \
&& cd /tmp/dbt_test_not_null_proposals_proposal_id \
&& dbt deps && dbt test --select not_null_proposals_proposal_id \
&& rm -r /tmp/dbt_test_not_null_proposals_proposal_id',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
        },
        append_env=True
    )

    column_completeness_test_source_proposals_proposal_id__id__bronze__decidim_proposals_proposals_task = BashOperator(
        task_id='test_column_completeness_test_source_proposals_proposal_id__id__bronze__decidim_proposals_proposals',
        bash_command='rm -r /tmp/dbt_test_column_completeness_test_source_proposals_proposal_id__id__bronze__decidim_proposals_proposals || true \
&& cp -r /opt/airflow/dags-config/repo/plugins/dbt_pg_project /tmp/dbt_test_column_completeness_test_source_proposals_proposal_id__id__bronze__decidim_proposals_proposals \
&& cd /tmp/dbt_test_column_completeness_test_source_proposals_proposal_id__id__bronze__decidim_proposals_proposals \
&& dbt deps && dbt test --select column_completeness_test_source_proposals_proposal_id__id__bronze__decidim_proposals_proposals \
&& rm -r /tmp/dbt_test_column_completeness_test_source_proposals_proposal_id__id__bronze__decidim_proposals_proposals',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
        },
        append_env=True
    )

    not_null_proposals_process_id_task = BashOperator(
        task_id='test_not_null_proposals_process_id',
        bash_command='rm -r /tmp/dbt_test_not_null_proposals_process_id || true \
&& cp -r /opt/airflow/dags-config/repo/plugins/dbt_pg_project /tmp/dbt_test_not_null_proposals_process_id \
&& cd /tmp/dbt_test_not_null_proposals_process_id \
&& dbt deps && dbt test --select not_null_proposals_process_id \
&& rm -r /tmp/dbt_test_not_null_proposals_process_id',
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
&& cd /tmp/dbt_test_referential_integrity_test_proposals_process_id__process_id__participatory_processes \
&& dbt deps && dbt test --select referential_integrity_test_proposals_process_id__process_id__participatory_processes \
&& rm -r /tmp/dbt_test_referential_integrity_test_proposals_process_id__process_id__participatory_processes',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
        },
        append_env=True
    )

    not_null_proposals_user_id_task = BashOperator(
        task_id='test_not_null_proposals_user_id',
        bash_command='rm -r /tmp/dbt_test_not_null_proposals_user_id || true \
&& cp -r /opt/airflow/dags-config/repo/plugins/dbt_pg_project /tmp/dbt_test_not_null_proposals_user_id \
&& cd /tmp/dbt_test_not_null_proposals_user_id \
&& dbt deps && dbt test --select not_null_proposals_user_id \
&& rm -r /tmp/dbt_test_not_null_proposals_user_id',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
        },
        append_env=True
    )

    referential_integrity_test_proposals_user_id__user_id__users_task = BashOperator(
        task_id='test_referential_integrity_test_proposals_user_id__user_id__users',
        bash_command='rm -r /tmp/dbt_test_referential_integrity_test_proposals_user_id__user_id__users || true \
&& cp -r /opt/airflow/dags-config/repo/plugins/dbt_pg_project /tmp/dbt_test_referential_integrity_test_proposals_user_id__user_id__users \
&& cd /tmp/dbt_test_referential_integrity_test_proposals_user_id__user_id__users \
&& dbt deps && dbt test --select referential_integrity_test_proposals_user_id__user_id__users \
&& rm -r /tmp/dbt_test_referential_integrity_test_proposals_user_id__user_id__users',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
        },
        append_env=True
    )

    referential_integrity_test_comments_commented_component_id__component_type_proposal___proposal_id__proposals_task = BashOperator(
        task_id='test_referential_integrity_test_comments_commented_component_id__component_type_proposal___proposal_id__proposals',
        bash_command='rm -r /tmp/dbt_test_referential_integrity_test_comments_commented_component_id__component_type_proposal___proposal_id__proposals || true \
&& cp -r /opt/airflow/dags-config/repo/plugins/dbt_pg_project /tmp/dbt_test_referential_integrity_test_comments_commented_component_id__component_type_proposal___proposal_id__proposals \
&& cd /tmp/dbt_test_referential_integrity_test_comments_commented_component_id__component_type_proposal___proposal_id__proposals \
&& dbt deps && dbt test --select referential_integrity_test_comments_commented_component_id__component_type_proposal___proposal_id__proposals \
&& rm -r /tmp/dbt_test_referential_integrity_test_comments_commented_component_id__component_type_proposal___proposal_id__proposals',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
        },
        append_env=True
    )

    referential_integrity_test_votes_voted_component_id__component_type_proposal___proposal_id__proposals_task = BashOperator(
        task_id='test_referential_integrity_test_votes_voted_component_id__component_type_proposal___proposal_id__proposals',
        bash_command='rm -r /tmp/dbt_test_referential_integrity_test_votes_voted_component_id__component_type_proposal___proposal_id__proposals || true \
&& cp -r /opt/airflow/dags-config/repo/plugins/dbt_pg_project /tmp/dbt_test_referential_integrity_test_votes_voted_component_id__component_type_proposal___proposal_id__proposals \
&& cd /tmp/dbt_test_referential_integrity_test_votes_voted_component_id__component_type_proposal___proposal_id__proposals \
&& dbt deps && dbt test --select referential_integrity_test_votes_voted_component_id__component_type_proposal___proposal_id__proposals \
&& rm -r /tmp/dbt_test_referential_integrity_test_votes_voted_component_id__component_type_proposal___proposal_id__proposals',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
        },
        append_env=True
    )

    proposals_task >> unique_proposals_proposal_id_task >> end_task
    proposals_task >> not_null_proposals_proposal_id_task >> end_task
    proposals_task >> column_completeness_test_source_proposals_proposal_id__id__bronze__decidim_proposals_proposals_task >> end_task
    proposals_task >> not_null_proposals_process_id_task >> end_task
    proposals_task >> referential_integrity_test_proposals_process_id__process_id__participatory_processes_task >> end_task
    proposals_task >> not_null_proposals_user_id_task >> end_task
    proposals_task >> referential_integrity_test_proposals_user_id__user_id__users_task >> end_task
    proposals_task >> referential_integrity_test_comments_commented_component_id__component_type_proposal___proposal_id__proposals_task >> end_task
    proposals_task >> referential_integrity_test_votes_voted_component_id__component_type_proposal___proposal_id__proposals_task >> end_task
