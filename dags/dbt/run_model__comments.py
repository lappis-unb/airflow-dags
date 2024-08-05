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
    "run_model__comments",
    default_args=default_args,
    schedule='@daily',
    start_date=days_ago(1),
    tags=["dbt", "model"],
) as dag:

    end_task = EmptyOperator(
        task_id="end",
        outlets=[Dataset("comments_model")],
    )

    comments_task = BashOperator(
        task_id='run_comments',
        bash_command='dbt deps && dbt run --select comments \
&& rm -r /tmp/dbt_target_run_comments /tmp/dbt_logs_run_comments',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
            'DBT_TARGET_PATH': '/tmp/dbt_target_run_comments',
            'DBT_LOG_PATH': '/tmp/dbt_logs_run_comments'
        },
        cwd='/opt/airflow/dags-config/repo/plugins/dbt_pg_project',
        append_env=True
    )

    unique_comments_comment_id_task = BashOperator(
        task_id='test_unique_comments_comment_id',
        bash_command='dbt deps && dbt test --select unique_comments_comment_id \
&& rm -r /tmp/dbt_target_test_unique_comments_comment_id /tmp/dbt_logs_test_unique_comments_comment_id',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
            'DBT_TARGET_PATH': '/tmp/dbt_target_test_unique_comments_comment_id',
            'DBT_LOG_PATH': '/tmp/dbt_logs_test_unique_comments_comment_id'
        },
        cwd='/opt/airflow/dags-config/repo/plugins/dbt_pg_project',
        append_env=True
    )

    not_null_comments_comment_id_task = BashOperator(
        task_id='test_not_null_comments_comment_id',
        bash_command='dbt deps && dbt test --select not_null_comments_comment_id \
&& rm -r /tmp/dbt_target_test_not_null_comments_comment_id /tmp/dbt_logs_test_not_null_comments_comment_id',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
            'DBT_TARGET_PATH': '/tmp/dbt_target_test_not_null_comments_comment_id',
            'DBT_LOG_PATH': '/tmp/dbt_logs_test_not_null_comments_comment_id'
        },
        cwd='/opt/airflow/dags-config/repo/plugins/dbt_pg_project',
        append_env=True
    )

    column_completeness_test_source_comments_comment_id__id__o_decidim_participatory_space_type_Decidim_ParticipatoryProcess___bronze__decidim_comments_comments_task = BashOperator(
        task_id='test_column_completeness_test_source_comments_comment_id__id__o_decidim_participatory_space_type_Decidim_ParticipatoryProcess___bronze__decidim_comments_comments',
        bash_command='dbt deps && dbt test --select column_completeness_test_source_comments_comment_id__id__o_decidim_participatory_space_type_Decidim_ParticipatoryProcess___bronze__decidim_comments_comments \
&& rm -r /tmp/dbt_target_test_column_completeness_test_source_comments_comment_id__id__o_decidim_participatory_space_type_Decidim_ParticipatoryProcess___bronze__decidim_comments_comments /tmp/dbt_logs_test_column_completeness_test_source_comments_comment_id__id__o_decidim_participatory_space_type_Decidim_ParticipatoryProcess___bronze__decidim_comments_comments',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
            'DBT_TARGET_PATH': '/tmp/dbt_target_test_column_completeness_test_source_comments_comment_id__id__o_decidim_participatory_space_type_Decidim_ParticipatoryProcess___bronze__decidim_comments_comments',
            'DBT_LOG_PATH': '/tmp/dbt_logs_test_column_completeness_test_source_comments_comment_id__id__o_decidim_participatory_space_type_Decidim_ParticipatoryProcess___bronze__decidim_comments_comments'
        },
        cwd='/opt/airflow/dags-config/repo/plugins/dbt_pg_project',
        append_env=True
    )

    not_null_comments_commented_component_id_task = BashOperator(
        task_id='test_not_null_comments_commented_component_id',
        bash_command='dbt deps && dbt test --select not_null_comments_commented_component_id \
&& rm -r /tmp/dbt_target_test_not_null_comments_commented_component_id /tmp/dbt_logs_test_not_null_comments_commented_component_id',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
            'DBT_TARGET_PATH': '/tmp/dbt_target_test_not_null_comments_commented_component_id',
            'DBT_LOG_PATH': '/tmp/dbt_logs_test_not_null_comments_commented_component_id'
        },
        cwd='/opt/airflow/dags-config/repo/plugins/dbt_pg_project',
        append_env=True
    )

    referential_integrity_test_comments_commented_component_id__component_type_proposal___proposal_id__proposals_task = BashOperator(
        task_id='test_referential_integrity_test_comments_commented_component_id__component_type_proposal___proposal_id__proposals',
        bash_command='dbt deps && dbt test --select referential_integrity_test_comments_commented_component_id__component_type_proposal___proposal_id__proposals \
&& rm -r /tmp/dbt_target_test_referential_integrity_test_comments_commented_component_id__component_type_proposal___proposal_id__proposals /tmp/dbt_logs_test_referential_integrity_test_comments_commented_component_id__component_type_proposal___proposal_id__proposals',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
            'DBT_TARGET_PATH': '/tmp/dbt_target_test_referential_integrity_test_comments_commented_component_id__component_type_proposal___proposal_id__proposals',
            'DBT_LOG_PATH': '/tmp/dbt_logs_test_referential_integrity_test_comments_commented_component_id__component_type_proposal___proposal_id__proposals'
        },
        cwd='/opt/airflow/dags-config/repo/plugins/dbt_pg_project',
        append_env=True
    )

    referential_integrity_test_comments_commented_component_id__component_type_comment___comment_id__comments_task = BashOperator(
        task_id='test_referential_integrity_test_comments_commented_component_id__component_type_comment___comment_id__comments',
        bash_command='dbt deps && dbt test --select referential_integrity_test_comments_commented_component_id__component_type_comment___comment_id__comments \
&& rm -r /tmp/dbt_target_test_referential_integrity_test_comments_commented_component_id__component_type_comment___comment_id__comments /tmp/dbt_logs_test_referential_integrity_test_comments_commented_component_id__component_type_comment___comment_id__comments',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
            'DBT_TARGET_PATH': '/tmp/dbt_target_test_referential_integrity_test_comments_commented_component_id__component_type_comment___comment_id__comments',
            'DBT_LOG_PATH': '/tmp/dbt_logs_test_referential_integrity_test_comments_commented_component_id__component_type_comment___comment_id__comments'
        },
        cwd='/opt/airflow/dags-config/repo/plugins/dbt_pg_project',
        append_env=True
    )

    not_null_comments_user_id_task = BashOperator(
        task_id='test_not_null_comments_user_id',
        bash_command='dbt deps && dbt test --select not_null_comments_user_id \
&& rm -r /tmp/dbt_target_test_not_null_comments_user_id /tmp/dbt_logs_test_not_null_comments_user_id',
        env={
            'DBT_POSTGRES_HOST': Variable.get("dbt_postgres_host"),
            'DBT_POSTGRES_USER': Variable.get("dbt_postgres_user"),
            'DBT_POSTGRES_PASSWORD': Variable.get("dbt_postgres_password"),
            'DBT_TARGET_PATH': '/tmp/dbt_target_test_not_null_comments_user_id',
            'DBT_LOG_PATH': '/tmp/dbt_logs_test_not_null_comments_user_id'
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

    comments_task >> unique_comments_comment_id_task >> end_task
    comments_task >> not_null_comments_comment_id_task >> end_task
    comments_task >> column_completeness_test_source_comments_comment_id__id__o_decidim_participatory_space_type_Decidim_ParticipatoryProcess___bronze__decidim_comments_comments_task >> end_task
    comments_task >> not_null_comments_commented_component_id_task >> end_task
    comments_task >> referential_integrity_test_comments_commented_component_id__component_type_proposal___proposal_id__proposals_task >> end_task
    comments_task >> referential_integrity_test_comments_commented_component_id__component_type_comment___comment_id__comments_task >> end_task
    comments_task >> not_null_comments_user_id_task >> end_task
    comments_task >> referential_integrity_test_comments_user_id__user_id__users_task >> end_task
    comments_task >> referential_integrity_test_votes_voted_component_id__component_type_comment___comment_id__comments_task >> end_task
