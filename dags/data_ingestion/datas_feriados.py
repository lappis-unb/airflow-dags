import time
from datetime import datetime
from io import StringIO

import pandas as pd
import requests
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    "owner": "Amoêdo",
}

dataset = Dataset("bronze_datas")

MESES = {
    1: "janeiro",
    2: "fevereiro",
    3: "março",
    4: "abril",
    5: "maio",
    6: "junho",
    7: "julho",
    8: "agosto",
    9: "setembro",
    10: "outubro",
    11: "novembro",
    12: "dezembro",
}

DIAS_SEMANA = {
    0: "segunda-feira",
    1: "terça-feira",
    2: "quarta-feira",
    3: "quinta-feira",
    4: "sexta-feira",
    5: "sábado",
    6: "domingo",
}

START_DATE = "1990-01-01"
END_DATE = "2030-12-31"
POSTGRES_CONN_ID = "pg_bp_analytics"
SCHEMA = "raw"
TABLE_DAT_NAME = "datas"
TABLE_FERIADOS_NAME = "feriados"


@dag(
    schedule_interval="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["data-ingestion"],
    default_args=default_args,
)
def datas_feriados():

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    def calc_num_semana_ano(num_dia_ano, num_dia_semana):
        num_semana_ano = (num_dia_ano - num_dia_semana + 10) // 7
        return num_semana_ano

    def load(df, table_name, schema):
        engine = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_sqlalchemy_engine()
        with engine.connect() as connection:
            connection.execute(f"DROP TABLE IF EXISTS {schema}.{table_name} CASCADE;")
        df.to_sql(table_name, con=engine, schema=schema, if_exists="replace", index=False)

    @task()
    def create_dataframe() -> pd.DataFrame:
        data_range = pd.date_range(start=START_DATE, end=END_DATE)
        df = pd.DataFrame(data_range, columns=["data"])
        df["ano_mes_dia"] = df["data"].dt.strftime("%Y%m%d")
        df["ano_mes"] = df["data"].dt.strftime("%Y%m")
        df["ano"] = df["data"].dt.strftime("%Y")
        df["descricao_dia_semana"] = df["data"].dt.weekday.map(DIAS_SEMANA)
        df["descricao_mes"] = df["data"].dt.month.map(MESES)
        df["numero_mes"] = df["data"].dt.month
        df["numero_dia_mes"] = df["data"].dt.day
        df["numero_dia_semana"] = (df["data"].dt.weekday + 1) % 7 + 1
        df["numero_dia_ano"] = df["data"].dt.dayofyear
        df["numero_semana_mes"] = df["data"].apply(lambda x: (x.day - 1) // 7 + 1)
        df["final_de_semana"] = df["numero_dia_semana"].isin([6, 7])
        df["numero_semana_ano"] = df.apply(
            lambda x: calc_num_semana_ano(x["numero_dia_ano"], x["numero_dia_semana"]), axis=1
        )
        df.drop(columns=["data"], inplace=True)
        return df

    @task()
    def load_series_datas(datas: pd.DataFrame):
        load(datas, TABLE_DAT_NAME, SCHEMA)

    @task()
    def get_feriados():
        start_date = int(START_DATE[:4])
        end_date = int(END_DATE[:4])

        url = "https://brasilapi.com.br/api/feriados/v1/{ano}"
        dfs = []
        for ano in range(start_date, end_date):
            response = requests.get(url.format(ano=ano))
            time.sleep(0.5)
            response.raise_for_status()
            data = response.text
            print(data)
            df = pd.read_json(StringIO(data))
            dfs.append(df)
            print(ano)

        df = pd.concat(dfs)
        return df

    @task(outlets=dataset)
    def load_feriados(feriados: pd.DataFrame):
        load(feriados, TABLE_FERIADOS_NAME, SCHEMA)

    _create_dataframe = create_dataframe()
    _load = load_series_datas(_create_dataframe)
    start >> _create_dataframe >> _load >> end

    _get_feriados = get_feriados()
    _load_feriados = load_feriados(_get_feriados)
    start >> _get_feriados >> _load_feriados >> end


datas_feriados()
