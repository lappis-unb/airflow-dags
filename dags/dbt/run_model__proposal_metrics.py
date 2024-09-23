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
    "run_model__proposal_metrics",
    default_args=default_args,
    schedule=[Dataset('proposals_model'), Dataset('participatory_processes_model'), Dataset('visits_model'), Dataset('votes_model'), Dataset('comments_model')],
    start_date=days_ago(1),
    tags=["dbt", "model"],
    max_active_runs=1
) as dag:

    end_task = EmptyOperator(
        task_id="end",
        outlets=[Dataset("proposal_metrics_model")],
    )

    proposal_metrics_task = BashOperator(
        task_id='run_proposal_metrics',
        bash_command='rm -r /tmp/dbt_run_proposal_metrics || true \
&& cp -r /opt/airflow/dags-config/repo/plugins/dbt_pg_project /tmp/dbt_run_proposal_metrics \
&& cd /tmp/dbt_run_proposal_metrics \
&& dbt deps && dbt run --select proposal_metrics \
&& rm -r /tmp/dbt_run_proposal_metrics',
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

    not_null_proposal_metrics_data_operacao_task = BashOperator(
        task_id='test_not_null_proposal_metrics_data_operacao',
        bash_command='rm -r /tmp/dbt_test_not_null_proposal_metrics_data_operacao || true \
&& cp -r /opt/airflow/dags-config/repo/plugins/dbt_pg_project /tmp/dbt_test_not_null_proposal_metrics_data_operacao \
&& cd /tmp/dbt_test_not_null_proposal_metrics_data_operacao \
&& dbt deps && dbt test --select not_null_proposal_metrics_data_operacao \
&& rm -r /tmp/dbt_test_not_null_proposal_metrics_data_operacao',
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

    not_null_proposal_metrics_id_processo_task = BashOperator(
        task_id='test_not_null_proposal_metrics_id_processo',
        bash_command='rm -r /tmp/dbt_test_not_null_proposal_metrics_id_processo || true \
&& cp -r /opt/airflow/dags-config/repo/plugins/dbt_pg_project /tmp/dbt_test_not_null_proposal_metrics_id_processo \
&& cd /tmp/dbt_test_not_null_proposal_metrics_id_processo \
&& dbt deps && dbt test --select not_null_proposal_metrics_id_processo \
&& rm -r /tmp/dbt_test_not_null_proposal_metrics_id_processo',
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

    not_null_proposal_metrics_id_proposta_task = BashOperator(
        task_id='test_not_null_proposal_metrics_id_proposta',
        bash_command='rm -r /tmp/dbt_test_not_null_proposal_metrics_id_proposta || true \
&& cp -r /opt/airflow/dags-config/repo/plugins/dbt_pg_project /tmp/dbt_test_not_null_proposal_metrics_id_proposta \
&& cd /tmp/dbt_test_not_null_proposal_metrics_id_proposta \
&& dbt deps && dbt test --select not_null_proposal_metrics_id_proposta \
&& rm -r /tmp/dbt_test_not_null_proposal_metrics_id_proposta',
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

    not_null_proposal_metrics_titulo_proposta_task = BashOperator(
        task_id='test_not_null_proposal_metrics_titulo_proposta',
        bash_command='rm -r /tmp/dbt_test_not_null_proposal_metrics_titulo_proposta || true \
&& cp -r /opt/airflow/dags-config/repo/plugins/dbt_pg_project /tmp/dbt_test_not_null_proposal_metrics_titulo_proposta \
&& cd /tmp/dbt_test_not_null_proposal_metrics_titulo_proposta \
&& dbt deps && dbt test --select not_null_proposal_metrics_titulo_proposta \
&& rm -r /tmp/dbt_test_not_null_proposal_metrics_titulo_proposta',
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

    not_null_proposal_metrics_titulo_processo_task = BashOperator(
        task_id='test_not_null_proposal_metrics_titulo_processo',
        bash_command='rm -r /tmp/dbt_test_not_null_proposal_metrics_titulo_processo || true \
&& cp -r /opt/airflow/dags-config/repo/plugins/dbt_pg_project /tmp/dbt_test_not_null_proposal_metrics_titulo_processo \
&& cd /tmp/dbt_test_not_null_proposal_metrics_titulo_processo \
&& dbt deps && dbt test --select not_null_proposal_metrics_titulo_processo \
&& rm -r /tmp/dbt_test_not_null_proposal_metrics_titulo_processo',
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

    not_null_proposal_metrics_qtd_rejeicao_task = BashOperator(
        task_id='test_not_null_proposal_metrics_qtd_rejeicao',
        bash_command='rm -r /tmp/dbt_test_not_null_proposal_metrics_qtd_rejeicao || true \
&& cp -r /opt/airflow/dags-config/repo/plugins/dbt_pg_project /tmp/dbt_test_not_null_proposal_metrics_qtd_rejeicao \
&& cd /tmp/dbt_test_not_null_proposal_metrics_qtd_rejeicao \
&& dbt deps && dbt test --select not_null_proposal_metrics_qtd_rejeicao \
&& rm -r /tmp/dbt_test_not_null_proposal_metrics_qtd_rejeicao',
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

    not_null_proposal_metrics_qtd_visitas_task = BashOperator(
        task_id='test_not_null_proposal_metrics_qtd_visitas',
        bash_command='rm -r /tmp/dbt_test_not_null_proposal_metrics_qtd_visitas || true \
&& cp -r /opt/airflow/dags-config/repo/plugins/dbt_pg_project /tmp/dbt_test_not_null_proposal_metrics_qtd_visitas \
&& cd /tmp/dbt_test_not_null_proposal_metrics_qtd_visitas \
&& dbt deps && dbt test --select not_null_proposal_metrics_qtd_visitas \
&& rm -r /tmp/dbt_test_not_null_proposal_metrics_qtd_visitas',
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

    not_null_proposal_metrics_qtd_votos_task = BashOperator(
        task_id='test_not_null_proposal_metrics_qtd_votos',
        bash_command='rm -r /tmp/dbt_test_not_null_proposal_metrics_qtd_votos || true \
&& cp -r /opt/airflow/dags-config/repo/plugins/dbt_pg_project /tmp/dbt_test_not_null_proposal_metrics_qtd_votos \
&& cd /tmp/dbt_test_not_null_proposal_metrics_qtd_votos \
&& dbt deps && dbt test --select not_null_proposal_metrics_qtd_votos \
&& rm -r /tmp/dbt_test_not_null_proposal_metrics_qtd_votos',
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

    not_null_proposal_metrics_qtd_comentarios_task = BashOperator(
        task_id='test_not_null_proposal_metrics_qtd_comentarios',
        bash_command='rm -r /tmp/dbt_test_not_null_proposal_metrics_qtd_comentarios || true \
&& cp -r /opt/airflow/dags-config/repo/plugins/dbt_pg_project /tmp/dbt_test_not_null_proposal_metrics_qtd_comentarios \
&& cd /tmp/dbt_test_not_null_proposal_metrics_qtd_comentarios \
&& dbt deps && dbt test --select not_null_proposal_metrics_qtd_comentarios \
&& rm -r /tmp/dbt_test_not_null_proposal_metrics_qtd_comentarios',
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

    proposal_metrics_task >> not_null_proposal_metrics_data_operacao_task >> end_task
    proposal_metrics_task >> not_null_proposal_metrics_id_processo_task >> end_task
    proposal_metrics_task >> not_null_proposal_metrics_id_proposta_task >> end_task
    proposal_metrics_task >> not_null_proposal_metrics_titulo_proposta_task >> end_task
    proposal_metrics_task >> not_null_proposal_metrics_titulo_processo_task >> end_task
    proposal_metrics_task >> not_null_proposal_metrics_qtd_rejeicao_task >> end_task
    proposal_metrics_task >> not_null_proposal_metrics_qtd_visitas_task >> end_task
    proposal_metrics_task >> not_null_proposal_metrics_qtd_votos_task >> end_task
    proposal_metrics_task >> not_null_proposal_metrics_qtd_comentarios_task >> end_task
