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

dataset = Dataset("bronze_dates")

MONTHS = {
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

WEEK_DAYS = {
    0: "segunda-feira",
    1: "terça-feira",
    2: "quarta-feira",
    3: "quinta-feira",
    4: "sexta-feira",
    5: "sábado",
    6: "domingo",
}

START_DATE = "2010-01-01"
END_DATE = "2050-12-31"
POSTGRES_CONN_ID = "pg_bp_analytics"
SCHEMA = "raw"
TABLE_DATES_NAME = "dates"
TABLE_HOLIDAYS_NAME = "holidays"


@dag(
    schedule_interval="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["data-ingestion"],
    default_args=default_args,
)
def dates_holidays():

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    def week_number_of_month(date, week_day_num):
        day_of_month = date.day
        # Adjust the day of the month so the week starts on Sunday and ends on Saturday
        adjusted_day = day_of_month + (7 - week_day_num) % 7
        return (adjusted_day - 1) // 7 + 1

    def calc_week_number_of_year(day_of_year, week_day_num):
        week_number_of_year = (day_of_year - week_day_num + 10) // 7
        return week_number_of_year

    def load(df, table_name, schema):
        engine = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_sqlalchemy_engine()
        with engine.connect() as connection:
            connection.execute(f"DROP TABLE IF EXISTS {schema}.{table_name} CASCADE;")
        df.to_sql(table_name, con=engine, schema=schema, if_exists="replace", index=False)

    @task()
    def create_dataframe() -> pd.DataFrame:
        date_range = pd.date_range(start=START_DATE, end=END_DATE)
        df = pd.DataFrame(date_range, columns=["date"])
        df["year_month_day"] = df["date"].dt.strftime("%Y%m%d")
        df["year_month"] = df["date"].dt.strftime("%Y%m")
        df["year"] = df["date"].dt.strftime("%Y")
        df["week_day_description"] = df["date"].dt.weekday.map(WEEK_DAYS)
        df["month_description"] = df["date"].dt.month.map(MONTHS)
        df["month_number"] = df["date"].dt.month
        df["day_of_month"] = df["date"].dt.day
        df["day_of_week"] = (df["date"].dt.weekday + 1) % 7 + 1
        df["day_of_year"] = df["date"].dt.dayofyear
        df["week_of_month"] = df.apply(
            lambda row: week_number_of_month(row["date"], row["day_of_week"]), axis=1
        )
        df["weekend"] = df["day_of_week"].isin([7, 1])
        df["week_of_year"] = df.apply(
            lambda x: calc_week_number_of_year(x["day_of_year"], x["day_of_week"]), axis=1
        )
        df.drop(columns=["date"], inplace=True)
        return df

    @task()
    def load_series_dates(dates: pd.DataFrame):
        load(dates, TABLE_DATES_NAME, SCHEMA)

    @task()
    def get_holidays():
        start_year = int(START_DATE[:4])
        end_year = int(END_DATE[:4])

        url = "https://brasilapi.com.br/api/feriados/v1/{year}"
        dfs = []
        for year in range(start_year, end_year + 1):
            response = requests.get(url.format(year=year))
            time.sleep(0.5)
            response.raise_for_status()
            data = response.text
            print(data)
            df = pd.read_json(StringIO(data))
            dfs.append(df)
            print(year)

        df = pd.concat(dfs)
        return df

    @task(outlets=dataset)
    def load_holidays(holidays: pd.DataFrame):
        load(holidays, TABLE_HOLIDAYS_NAME, SCHEMA)

    _create_dataframe = create_dataframe()
    _load = load_series_dates(_create_dataframe)
    start >> _create_dataframe >> _load >> end

    _get_holidays = get_holidays()
    _load_holidays = load_holidays(_get_holidays)
    start >> _get_holidays >> _load_holidays >> end


dates_holidays()
