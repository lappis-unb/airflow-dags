import pandas as pd
from minio import Minio
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.hooks.base_hook import BaseHook
from airflow.sensors.external_task_sensor import ExternalTaskSensor

# constante
MINIO_BUCKET = 'brasil-participativo-daily-csv'
MINIO_CONN_ID = "minio_connection_id"

def add_temporal_columns(df: pd.DataFrame, execution_date: datetime) -> pd.DataFrame:
    """
    Adds temporal columns to the DataFrame based on the execution date.

    Args:
    ----
        df (pd.DataFrame): The original DataFrame without temporal columns.
        execution_date (datetime): The execution date to base the temporal columns on.

    Returns:
    -------
        pd.DataFrame: The DataFrame with added temporal columns.
    """
    event_day_id = int(execution_date.strftime("%Y%m%d"))
    available_day = execution_date + timedelta(days=1)
    available_day_id = int(available_day.strftime("%Y%m%d"))
    available_month_id = int(available_day.strftime("%Y%m"))
    available_year_id = int(available_day.strftime("%Y"))
    writing_day_id = int(datetime.now().strftime("%Y%m%d"))

    # Add the temporal columns to the DataFrame
    df["event_day_id"] = event_day_id
    df["available_day_id"] = available_day_id
    df["available_month_id"] = available_month_id
    df["available_year_id"] = available_year_id
    df["writing_day_id"] = writing_day_id

    return df

def _get_minio():
    """
    Retorna um cliente S3 configurado para se conectar ao MinIO.

    Retorna:
        s3_client (boto3.client): Cliente S3 configurado para se conectar ao MinIO.
    """
    minio_conn = BaseHook.get_connection(MINIO_CONN_ID)
    minio_host = minio_conn.host.split('//')[1]
    minio_access_key = minio_conn.login
    minio_secret_access = minio_conn.password
    client = Minio(
            minio_host,
            access_key=minio_access_key,
            secret_key=minio_secret_access,
            secure=False,
            )
    return client

@dag(
    default_args={
        "owner": "Amoêdo/Nitai",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    },
    schedule="0 23 * * *",
    catchup=False,
    start_date=datetime(2023, 11, 10),
    description=__doc__,
    tags=["decidim", "minio"],
)
def ingest_proposals_proccesses():
    """
    DAG que extrai os dados tratados do MinIO e os insere em um banco de dados PostgreSQL.
    """
    start = ExternalTaskSensor(
        external_dag_id = 'fetch_process_and_clean_proposals',
        #external_task_id = 'end',
        task_id = 'start')
    @task(provide_context=True, retries=3, retry_delay=timedelta(seconds=5))
    def get_data_from_minio(**context):
        """
        Retrieves data from Minio and performs additional processing.

        Args:
            **context: Additional context provided by Airflow.

        Raises:
            Exception: If there is an error retrieving or processing the data.
        """
        data = context['execution_date'].strftime('%Y%m%d')
        # servidor de arquivos
        minio = _get_minio()
        # pega o dado do minio
        dado = minio.get_object(MINIO_BUCKET, f'processed/proposals{data}.csv')
        df = pd.read_csv(dado)
        df = add_temporal_columns(df, context['execution_date'])
        # falta adicionar a inserção no banco de dados

    start >> get_data_from_minio()

ingest_proposals_proccesses()
