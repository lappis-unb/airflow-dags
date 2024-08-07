from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from datetime import datetime

import pandas as pd


periods = ["@daily", "@weekly", "@monthly"]

default_args_dag = {
    "owner": "Eric"
}


def generate_dag_matomo(period):

    @dag(
        description="Ingestões do matomo",
        dag_id= f"general_matomo_ingestion_{period.replace('@', '')}",
        default_args=default_args_dag,
        schedule_interval=period,
        tags=["extract", "ingestion", "segmentation", "matomo", period.replace('@', '')],
        start_date=datetime(2024, 8, 1)
    )
    def matomo_data_extraction():
        
        start = EmptyOperator(task_id="start")


        end = EmptyOperator(task_id="end")

        start >> end

    return matomo_data_extraction()

for row in periods:
    globals()[f"matomo_ingestion_{row.replace('@', '')}"] = generate_dag_matomo(row)
