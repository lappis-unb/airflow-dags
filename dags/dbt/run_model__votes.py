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
    "run_model__votes",
    default_args=default_args,
    schedule='@daily',
    start_date=days_ago(1),
    tags=["dbt", "model"],
) as dag:

    end_task = EmptyOperator(
        task_id="end",
        outlets=[Dataset("votes_model")],
    )

    votes_task = BashOperator(
        task_id='run_votes',
        bash_command='dbt deps && dbt run --select votes \
&& rm -r /tmp/dbt_target_run_votes /tmp/dbt_logs_run_votes',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
            'DBT_TARGET_PATH': '/tmp/dbt_target_run_votes',
            'DBT_LOG_PATH': '/tmp/dbt_logs_run_votes'
        },
        cwd='/opt/airflow/dags-config/repo/plugins/dbt_pg_project',
        append_env=True
    )

    column_completeness_test_source_votes_original_vote_id__m_component_type_proposal___id__bronze__decidim_proposals_proposal_votes_task = BashOperator(
        task_id='test_column_completeness_test_source_votes_original_vote_id__m_component_type_proposal___id__bronze__decidim_proposals_proposal_votes',
        bash_command='dbt deps && dbt test --select column_completeness_test_source_votes_original_vote_id__m_component_type_proposal___id__bronze__decidim_proposals_proposal_votes \
&& rm -r /tmp/dbt_target_test_column_completeness_test_source_votes_original_vote_id__m_component_type_proposal___id__bronze__decidim_proposals_proposal_votes /tmp/dbt_logs_test_column_completeness_test_source_votes_original_vote_id__m_component_type_proposal___id__bronze__decidim_proposals_proposal_votes',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
            'DBT_TARGET_PATH': '/tmp/dbt_target_test_column_completeness_test_source_votes_original_vote_id__m_component_type_proposal___id__bronze__decidim_proposals_proposal_votes',
            'DBT_LOG_PATH': '/tmp/dbt_logs_test_column_completeness_test_source_votes_original_vote_id__m_component_type_proposal___id__bronze__decidim_proposals_proposal_votes'
        },
        cwd='/opt/airflow/dags-config/repo/plugins/dbt_pg_project',
        append_env=True
    )

    column_completeness_test_source_votes_original_vote_id__m_component_type_comment___id__bronze__decidim_comments_comment_votes_task = BashOperator(
        task_id='test_column_completeness_test_source_votes_original_vote_id__m_component_type_comment___id__bronze__decidim_comments_comment_votes',
        bash_command='dbt deps && dbt test --select column_completeness_test_source_votes_original_vote_id__m_component_type_comment___id__bronze__decidim_comments_comment_votes \
&& rm -r /tmp/dbt_target_test_column_completeness_test_source_votes_original_vote_id__m_component_type_comment___id__bronze__decidim_comments_comment_votes /tmp/dbt_logs_test_column_completeness_test_source_votes_original_vote_id__m_component_type_comment___id__bronze__decidim_comments_comment_votes',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
            'DBT_TARGET_PATH': '/tmp/dbt_target_test_column_completeness_test_source_votes_original_vote_id__m_component_type_comment___id__bronze__decidim_comments_comment_votes',
            'DBT_LOG_PATH': '/tmp/dbt_logs_test_column_completeness_test_source_votes_original_vote_id__m_component_type_comment___id__bronze__decidim_comments_comment_votes'
        },
        cwd='/opt/airflow/dags-config/repo/plugins/dbt_pg_project',
        append_env=True
    )

    unique_votes_vote_id_task = BashOperator(
        task_id='test_unique_votes_vote_id',
        bash_command='dbt deps && dbt test --select unique_votes_vote_id \
&& rm -r /tmp/dbt_target_test_unique_votes_vote_id /tmp/dbt_logs_test_unique_votes_vote_id',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
            'DBT_TARGET_PATH': '/tmp/dbt_target_test_unique_votes_vote_id',
            'DBT_LOG_PATH': '/tmp/dbt_logs_test_unique_votes_vote_id'
        },
        cwd='/opt/airflow/dags-config/repo/plugins/dbt_pg_project',
        append_env=True
    )

    not_null_votes_vote_id_task = BashOperator(
        task_id='test_not_null_votes_vote_id',
        bash_command='dbt deps && dbt test --select not_null_votes_vote_id \
&& rm -r /tmp/dbt_target_test_not_null_votes_vote_id /tmp/dbt_logs_test_not_null_votes_vote_id',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
            'DBT_TARGET_PATH': '/tmp/dbt_target_test_not_null_votes_vote_id',
            'DBT_LOG_PATH': '/tmp/dbt_logs_test_not_null_votes_vote_id'
        },
        cwd='/opt/airflow/dags-config/repo/plugins/dbt_pg_project',
        append_env=True
    )

    not_null_votes_voted_component_id_task = BashOperator(
        task_id='test_not_null_votes_voted_component_id',
        bash_command='dbt deps && dbt test --select not_null_votes_voted_component_id \
&& rm -r /tmp/dbt_target_test_not_null_votes_voted_component_id /tmp/dbt_logs_test_not_null_votes_voted_component_id',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
            'DBT_TARGET_PATH': '/tmp/dbt_target_test_not_null_votes_voted_component_id',
            'DBT_LOG_PATH': '/tmp/dbt_logs_test_not_null_votes_voted_component_id'
        },
        cwd='/opt/airflow/dags-config/repo/plugins/dbt_pg_project',
        append_env=True
    )

    referential_integrity_test_votes_voted_component_id__component_type_proposal___proposal_id__proposals_task = BashOperator(
        task_id='test_referential_integrity_test_votes_voted_component_id__component_type_proposal___proposal_id__proposals',
        bash_command='dbt deps && dbt test --select referential_integrity_test_votes_voted_component_id__component_type_proposal___proposal_id__proposals \
&& rm -r /tmp/dbt_target_test_referential_integrity_test_votes_voted_component_id__component_type_proposal___proposal_id__proposals /tmp/dbt_logs_test_referential_integrity_test_votes_voted_component_id__component_type_proposal___proposal_id__proposals',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
            'DBT_TARGET_PATH': '/tmp/dbt_target_test_referential_integrity_test_votes_voted_component_id__component_type_proposal___proposal_id__proposals',
            'DBT_LOG_PATH': '/tmp/dbt_logs_test_referential_integrity_test_votes_voted_component_id__component_type_proposal___proposal_id__proposals'
        },
        cwd='/opt/airflow/dags-config/repo/plugins/dbt_pg_project',
        append_env=True
    )

    referential_integrity_test_votes_voted_component_id__component_type_comment___comment_id__comments_task = BashOperator(
        task_id='test_referential_integrity_test_votes_voted_component_id__component_type_comment___comment_id__comments',
        bash_command='dbt deps && dbt test --select referential_integrity_test_votes_voted_component_id__component_type_comment___comment_id__comments \
&& rm -r /tmp/dbt_target_test_referential_integrity_test_votes_voted_component_id__component_type_comment___comment_id__comments /tmp/dbt_logs_test_referential_integrity_test_votes_voted_component_id__component_type_comment___comment_id__comments',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
            'DBT_TARGET_PATH': '/tmp/dbt_target_test_referential_integrity_test_votes_voted_component_id__component_type_comment___comment_id__comments',
            'DBT_LOG_PATH': '/tmp/dbt_logs_test_referential_integrity_test_votes_voted_component_id__component_type_comment___comment_id__comments'
        },
        cwd='/opt/airflow/dags-config/repo/plugins/dbt_pg_project',
        append_env=True
    )

    not_null_votes_user_id_task = BashOperator(
        task_id='test_not_null_votes_user_id',
        bash_command='dbt deps && dbt test --select not_null_votes_user_id \
&& rm -r /tmp/dbt_target_test_not_null_votes_user_id /tmp/dbt_logs_test_not_null_votes_user_id',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
            'DBT_TARGET_PATH': '/tmp/dbt_target_test_not_null_votes_user_id',
            'DBT_LOG_PATH': '/tmp/dbt_logs_test_not_null_votes_user_id'
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

    votes_task >> column_completeness_test_source_votes_original_vote_id__m_component_type_proposal___id__bronze__decidim_proposals_proposal_votes_task >> end_task
    votes_task >> column_completeness_test_source_votes_original_vote_id__m_component_type_comment___id__bronze__decidim_comments_comment_votes_task >> end_task
    votes_task >> unique_votes_vote_id_task >> end_task
    votes_task >> not_null_votes_vote_id_task >> end_task
    votes_task >> not_null_votes_voted_component_id_task >> end_task
    votes_task >> referential_integrity_test_votes_voted_component_id__component_type_proposal___proposal_id__proposals_task >> end_task
    votes_task >> referential_integrity_test_votes_voted_component_id__component_type_comment___comment_id__comments_task >> end_task
    votes_task >> not_null_votes_user_id_task >> end_task
    votes_task >> referential_integrity_test_votes_user_id__user_id__users_task >> end_task
