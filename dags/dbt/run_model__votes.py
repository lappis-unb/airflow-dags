# fmt: off
# ruff: noqa

from airflow import DAG
from airflow.datasets import Dataset
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

from datetime import timedelta

default_args = {
    'owner': 'DBT-Genrated',
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
    'pool': 'dbt_pool'
}

with DAG(
    "run_model__votes",
    default_args=default_args,
    schedule=[Dataset('bronze_decidim_proposals_proposal_votes'), Dataset('bronze_decidim_comments_comments'), Dataset('bronze_decidim_comments_comment_votes')],
    start_date=days_ago(1),
    tags=["dbt", "model"],
    max_active_runs=1
) as dag:

    end_task = EmptyOperator(
        task_id="end",
        outlets=[Dataset("votes_model")],
    )

    votes_task = BashOperator(
        task_id='run_votes',
        bash_command='rm -r /tmp/dbt_run_votes || true \
&& cp -r /opt/airflow/dags-config/repo/plugins/dbt_pg_project /tmp/dbt_run_votes \
&& cd /tmp/dbt_run_votes \
&& dbt deps && dbt run --select votes \
&& rm -r /tmp/dbt_run_votes',
        env={
            'DBT_POSTGRES_HOST': Variable.get("bp_dw_pg_host"),
            'DBT_POSTGRES_USER': Variable.get("bp_dw_pg_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("bp_dw_pg_password"),
            'DBT_POSTGRES_ENVIRONMENT': Variable.get("bp_dw_pg_environment"),
            'DBT_POSTGRES_PORT': Variable.get("bp_dw_pg_port"),
            'DBT_POSTGRES_DATABASE': Variable.get("bp_dw_pg_db"),
        },
        append_env=True
    )

    column_completeness_test_source_votes_original_vote_id__m_component_type_proposal___id__bronze__decidim_proposals_proposal_votes_task = BashOperator(
        task_id='test_column_completeness_test_source_votes_original_vote_id__m_component_type_proposal___id__bronze__decidim_proposals_proposal_votes',
        bash_command='rm -r /tmp/dbt_test_column_completeness_test_source_votes_original_vote_id__m_component_type_proposal___id__bronze__decidim_proposals_proposal_votes || true \
&& cp -r /opt/airflow/dags-config/repo/plugins/dbt_pg_project /tmp/dbt_test_column_completeness_test_source_votes_original_vote_id__m_component_type_proposal___id__bronze__decidim_proposals_proposal_votes \
&& cd /tmp/dbt_test_column_completeness_test_source_votes_original_vote_id__m_component_type_proposal___id__bronze__decidim_proposals_proposal_votes \
&& dbt deps && dbt test --select column_completeness_test_source_votes_original_vote_id__m_component_type_proposal___id__bronze__decidim_proposals_proposal_votes \
&& rm -r /tmp/dbt_test_column_completeness_test_source_votes_original_vote_id__m_component_type_proposal___id__bronze__decidim_proposals_proposal_votes',
        env={
            'DBT_POSTGRES_HOST': Variable.get("bp_dw_pg_host"),
            'DBT_POSTGRES_USER': Variable.get("bp_dw_pg_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("bp_dw_pg_password"),
            'DBT_POSTGRES_ENVIRONMENT': Variable.get("bp_dw_pg_environment"),
            'DBT_POSTGRES_PORT': Variable.get("bp_dw_pg_port"),
            'DBT_POSTGRES_DATABASE': Variable.get("bp_dw_pg_db"),
        },
        append_env=True
    )

    column_completeness_test_source_votes_original_vote_id__m_component_type_comment___id__bronze__decidim_comments_comment_votes_task = BashOperator(
        task_id='test_column_completeness_test_source_votes_original_vote_id__m_component_type_comment___id__bronze__decidim_comments_comment_votes',
        bash_command='rm -r /tmp/dbt_test_column_completeness_test_source_votes_original_vote_id__m_component_type_comment___id__bronze__decidim_comments_comment_votes || true \
&& cp -r /opt/airflow/dags-config/repo/plugins/dbt_pg_project /tmp/dbt_test_column_completeness_test_source_votes_original_vote_id__m_component_type_comment___id__bronze__decidim_comments_comment_votes \
&& cd /tmp/dbt_test_column_completeness_test_source_votes_original_vote_id__m_component_type_comment___id__bronze__decidim_comments_comment_votes \
&& dbt deps && dbt test --select column_completeness_test_source_votes_original_vote_id__m_component_type_comment___id__bronze__decidim_comments_comment_votes \
&& rm -r /tmp/dbt_test_column_completeness_test_source_votes_original_vote_id__m_component_type_comment___id__bronze__decidim_comments_comment_votes',
        env={
            'DBT_POSTGRES_HOST': Variable.get("bp_dw_pg_host"),
            'DBT_POSTGRES_USER': Variable.get("bp_dw_pg_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("bp_dw_pg_password"),
            'DBT_POSTGRES_ENVIRONMENT': Variable.get("bp_dw_pg_environment"),
            'DBT_POSTGRES_PORT': Variable.get("bp_dw_pg_port"),
            'DBT_POSTGRES_DATABASE': Variable.get("bp_dw_pg_db"),
        },
        append_env=True
    )

    unique_votes_vote_id_task = BashOperator(
        task_id='test_unique_votes_vote_id',
        bash_command='rm -r /tmp/dbt_test_unique_votes_vote_id || true \
&& cp -r /opt/airflow/dags-config/repo/plugins/dbt_pg_project /tmp/dbt_test_unique_votes_vote_id \
&& cd /tmp/dbt_test_unique_votes_vote_id \
&& dbt deps && dbt test --select unique_votes_vote_id \
&& rm -r /tmp/dbt_test_unique_votes_vote_id',
        env={
            'DBT_POSTGRES_HOST': Variable.get("bp_dw_pg_host"),
            'DBT_POSTGRES_USER': Variable.get("bp_dw_pg_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("bp_dw_pg_password"),
            'DBT_POSTGRES_ENVIRONMENT': Variable.get("bp_dw_pg_environment"),
            'DBT_POSTGRES_PORT': Variable.get("bp_dw_pg_port"),
            'DBT_POSTGRES_DATABASE': Variable.get("bp_dw_pg_db"),
        },
        append_env=True
    )

    not_null_votes_vote_id_task = BashOperator(
        task_id='test_not_null_votes_vote_id',
        bash_command='rm -r /tmp/dbt_test_not_null_votes_vote_id || true \
&& cp -r /opt/airflow/dags-config/repo/plugins/dbt_pg_project /tmp/dbt_test_not_null_votes_vote_id \
&& cd /tmp/dbt_test_not_null_votes_vote_id \
&& dbt deps && dbt test --select not_null_votes_vote_id \
&& rm -r /tmp/dbt_test_not_null_votes_vote_id',
        env={
            'DBT_POSTGRES_HOST': Variable.get("bp_dw_pg_host"),
            'DBT_POSTGRES_USER': Variable.get("bp_dw_pg_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("bp_dw_pg_password"),
            'DBT_POSTGRES_ENVIRONMENT': Variable.get("bp_dw_pg_environment"),
            'DBT_POSTGRES_PORT': Variable.get("bp_dw_pg_port"),
            'DBT_POSTGRES_DATABASE': Variable.get("bp_dw_pg_db"),
        },
        append_env=True
    )

    not_null_votes_voted_component_id_task = BashOperator(
        task_id='test_not_null_votes_voted_component_id',
        bash_command='rm -r /tmp/dbt_test_not_null_votes_voted_component_id || true \
&& cp -r /opt/airflow/dags-config/repo/plugins/dbt_pg_project /tmp/dbt_test_not_null_votes_voted_component_id \
&& cd /tmp/dbt_test_not_null_votes_voted_component_id \
&& dbt deps && dbt test --select not_null_votes_voted_component_id \
&& rm -r /tmp/dbt_test_not_null_votes_voted_component_id',
        env={
            'DBT_POSTGRES_HOST': Variable.get("bp_dw_pg_host"),
            'DBT_POSTGRES_USER': Variable.get("bp_dw_pg_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("bp_dw_pg_password"),
            'DBT_POSTGRES_ENVIRONMENT': Variable.get("bp_dw_pg_environment"),
            'DBT_POSTGRES_PORT': Variable.get("bp_dw_pg_port"),
            'DBT_POSTGRES_DATABASE': Variable.get("bp_dw_pg_db"),
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
            'DBT_POSTGRES_HOST': Variable.get("bp_dw_pg_host"),
            'DBT_POSTGRES_USER': Variable.get("bp_dw_pg_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("bp_dw_pg_password"),
            'DBT_POSTGRES_ENVIRONMENT': Variable.get("bp_dw_pg_environment"),
            'DBT_POSTGRES_PORT': Variable.get("bp_dw_pg_port"),
            'DBT_POSTGRES_DATABASE': Variable.get("bp_dw_pg_db"),
        },
        append_env=True
    )

    referential_integrity_test_votes_voted_component_id__component_type_comment___comment_id__comments_task = BashOperator(
        task_id='test_referential_integrity_test_votes_voted_component_id__component_type_comment___comment_id__comments',
        bash_command='rm -r /tmp/dbt_test_referential_integrity_test_votes_voted_component_id__component_type_comment___comment_id__comments || true \
&& cp -r /opt/airflow/dags-config/repo/plugins/dbt_pg_project /tmp/dbt_test_referential_integrity_test_votes_voted_component_id__component_type_comment___comment_id__comments \
&& cd /tmp/dbt_test_referential_integrity_test_votes_voted_component_id__component_type_comment___comment_id__comments \
&& dbt deps && dbt test --select referential_integrity_test_votes_voted_component_id__component_type_comment___comment_id__comments \
&& rm -r /tmp/dbt_test_referential_integrity_test_votes_voted_component_id__component_type_comment___comment_id__comments',
        env={
            'DBT_POSTGRES_HOST': Variable.get("bp_dw_pg_host"),
            'DBT_POSTGRES_USER': Variable.get("bp_dw_pg_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("bp_dw_pg_password"),
            'DBT_POSTGRES_ENVIRONMENT': Variable.get("bp_dw_pg_environment"),
            'DBT_POSTGRES_PORT': Variable.get("bp_dw_pg_port"),
            'DBT_POSTGRES_DATABASE': Variable.get("bp_dw_pg_db"),
        },
        append_env=True
    )

    not_null_votes_user_id_task = BashOperator(
        task_id='test_not_null_votes_user_id',
        bash_command='rm -r /tmp/dbt_test_not_null_votes_user_id || true \
&& cp -r /opt/airflow/dags-config/repo/plugins/dbt_pg_project /tmp/dbt_test_not_null_votes_user_id \
&& cd /tmp/dbt_test_not_null_votes_user_id \
&& dbt deps && dbt test --select not_null_votes_user_id \
&& rm -r /tmp/dbt_test_not_null_votes_user_id',
        env={
            'DBT_POSTGRES_HOST': Variable.get("bp_dw_pg_host"),
            'DBT_POSTGRES_USER': Variable.get("bp_dw_pg_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("bp_dw_pg_password"),
            'DBT_POSTGRES_ENVIRONMENT': Variable.get("bp_dw_pg_environment"),
            'DBT_POSTGRES_PORT': Variable.get("bp_dw_pg_port"),
            'DBT_POSTGRES_DATABASE': Variable.get("bp_dw_pg_db"),
        },
        append_env=True
    )

    referential_integrity_test_votes_user_id__user_id__users_task = BashOperator(
        task_id='test_referential_integrity_test_votes_user_id__user_id__users',
        bash_command='rm -r /tmp/dbt_test_referential_integrity_test_votes_user_id__user_id__users || true \
&& cp -r /opt/airflow/dags-config/repo/plugins/dbt_pg_project /tmp/dbt_test_referential_integrity_test_votes_user_id__user_id__users \
&& cd /tmp/dbt_test_referential_integrity_test_votes_user_id__user_id__users \
&& dbt deps && dbt test --select referential_integrity_test_votes_user_id__user_id__users \
&& rm -r /tmp/dbt_test_referential_integrity_test_votes_user_id__user_id__users',
        env={
            'DBT_POSTGRES_HOST': Variable.get("bp_dw_pg_host"),
            'DBT_POSTGRES_USER': Variable.get("bp_dw_pg_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("bp_dw_pg_password"),
            'DBT_POSTGRES_ENVIRONMENT': Variable.get("bp_dw_pg_environment"),
            'DBT_POSTGRES_PORT': Variable.get("bp_dw_pg_port"),
            'DBT_POSTGRES_DATABASE': Variable.get("bp_dw_pg_db"),
        },
        append_env=True
    )

    votes_task >> column_completeness_test_source_votes_original_vote_id__m_component_type_proposal___id__bronze__decidim_proposals_proposal_votes_task >> end_task
    votes_task >> column_completeness_test_source_votes_original_vote_id__m_component_type_comment___id__bronze__decidim_comments_comment_votes_task >> end_task
    votes_task >> unique_votes_vote_id_task >> end_task
    votes_task >> not_null_votes_vote_id_task >> end_task
    votes_task >> not_null_votes_voted_component_id_task >> end_task
    votes_task >> referential_integrity_test_votes_voted_component_id__component_type_proposal___proposal_id__proposals_task >> end_task
    votes_task >> referential_integrity_test_votes_voted_component_id__component_type_comment___comment_id__comments_task >> end_task
    votes_task >> not_null_votes_user_id_task >> end_task
    votes_task >> referential_integrity_test_votes_user_id__user_id__users_task >> end_task
