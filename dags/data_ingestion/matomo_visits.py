from datetime import timedelta

from airflow.datasets import Dataset
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.python import PythonVirtualenvOperator
from airflow.utils.dates import days_ago

doc_md_dag = """

## Documentação da DAG `data_ingestion_matomo_detailed_visits`

### Descrição

A DAG `data_ingestion_matomo_detailed_visits` é responsável pela extração e ingestão de detalhes
de visitas do Matomo, essa DAG coleta informações detalhadas sobre as visitas ao site, como origem
de tráfego, informações do dispositivo e detalhes de ações do usuário, e as armazena em uma tabela
na base de dados especificada.

### Detalhes da DAG

- **ID da DAG**: `matomo_detailed_visits_ingestion`
- **Tags**: `ingestion`
- **Proprietário**: `data`
- **Agendamento**: Diariamente às 04:00 UTC (`0 4 * * *`)
- **Data de início**: Configurada para iniciar um dia atrás (`days_ago(1)`)
- **Catchup**: Desativado (`False`)
- **Concurrency**: 1 (apenas uma execução da DAG por vez)
- **Renderização de templates como objetos nativos**: Ativado (`True`)

### Argumentos Padrão

- **Retries**: 2 (número de tentativas em caso de falha)
- **Retry Delay**: 10 minutos (tempo de espera entre as tentativas)

### Variáveis Utilizadas

- `bp_dw_pg_host`: Host do banco de dados PostgreSQL.
- `bp_dw_pg_port`: Porta do banco de dados PostgreSQL.
- `bp_dw_pg_user`: Usuário do banco de dados PostgreSQL.
- `bp_dw_pg_password`: Senha do banco de dados PostgreSQL.
- `bp_dw_pg_db`: Nome do banco de dados PostgreSQL.
- `matomo_api_token`: Token de autenticação para a API Matomo.

### Estrutura da DAG

A DAG é composta por duas tarefas principais:

#### 1. `extract_data`

**Operador**: `PythonVirtualenvOperator`

- **Descrição**: Extrai os detalhes das visitas do Matomo Live! API dentro de um intervalo
de datas especificado.
- **Parâmetros de Entrada**:
  - `api_url`: URL base da instância Matomo.
  - `site_id`: ID do site para o qual os dados serão recuperados.
  - `api_token`: Token de autenticação da API.
  - `start_date`: Data de início no formato `YYYY-MM-DD`.
  - `end_date`: Data de término no formato `YYYY-MM-DD`.
  - `limit`: Número de registros a serem buscados por página (padrão 100).
- **Bibliotecas Necessárias**: Não há requisitos adicionais além das bibliotecas do sistema.
- **Saída**: Retorna uma lista de detalhes das visitas em formato JSON.

#### 2. `write_data`

**Operador**: `PythonVirtualenvOperator`

- **Descrição**: Escreve os dados extraídos no banco de dados PostgreSQL. A tarefa remove
entradas existentes na tabela com base nas datas dos dados extraídos e insere as novas entradas.
- **Parâmetros de Entrada**:
  - `data`: Dados extraídos da tarefa anterior.
  - `extraction`: Nome da tabela onde os dados serão inseridos.
  - `schema`: Esquema do banco de dados.
  - `db_conn`: Dicionário com as informações de conexão ao banco de dados.
- **Bibliotecas Necessárias**: `pandas`, `sqlalchemy`
- **Saída**: Escreve os dados na tabela `raw.matomo_detailed_visits`.

#### Conexões e Relacionamentos

- A tarefa `extract_data` é executada primeiro, e seus dados são passados para a tarefa
`write_data`, que então escreve os dados no banco de dados.

### Conexões com Datasets

- **Output Dataset**: `bronze_matomo_detailed_visits`

Este dataset é marcado como uma saída da tarefa `write_data`, indicando que o processo de
ingestão de visitas detalhadas do Matomo foi concluído com sucesso.

"""

destination_db_conn = {
    "pg_host": Variable.get("bp_dw_pg_host"),
    "pg_port": int(Variable.get("bp_dw_pg_port")),
    "pg_user": Variable.get("bp_dw_pg_user"),
    "pg_password": Variable.get("bp_dw_pg_password"),
    "pg_db": Variable.get("bp_dw_pg_db"),
}

default_args = {"owner": "data", "retries": 2, "retry_delay": timedelta(minutes=10)}

api_url = "https://ew.dataprev.gov.br/index.php"
site_id = "18"
api_token = Variable.get("matomo_api_token")

start_date = "{{ ds }}"
end_date = "{{ macros.ds_add(ds, 1) }}"
limit = 100


@dag(
    dag_id="matomo_detailed_visits_ingestion",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    start_date=days_ago(1),
    tags=["ingestion"],
    catchup=False,
    concurrency=1,
    render_template_as_native_obj=True,
    doc_md=doc_md_dag,
)
def data_ingestion_matomo_detailed_visits():
    def fetch_visits_details(api_url, site_id, api_token, start_date, end_date, limit=100):
        """
        Fetches visitor details from Matomo Live! API within a specified date range, paginated.

        Parameters:
        ----------
            api_url (str): Base URL of the Matomo instance.
            site_id (str): The ID of the website to retrieve data for.
            api_token (str): API authentication token.
            start_date (str): Start date in 'YYYY-MM-DD' format.
            end_date (str): End date in 'YYYY-MM-DD' format.
            limit (int): Number of records to fetch per page (default is 100).

        Returns:
        -------
            list: A list of all visit details in JSON format.
        """
        import requests

        all_visits = []
        offset = 0

        while True:
            params = {
                "module": "API",
                "method": "Live.getLastVisitsDetails",
                "idSite": site_id,
                "period": "range",  # Fetch data within the date range
                "date": f"{start_date},{end_date}",
                "format": "JSON",
                "filter_limit": limit,
                "filter_offset": offset,
                "token_auth": api_token,
            }

            response = requests.get(api_url, params=params)

            # Check if the request was successful
            if response.status_code == 200:
                visits = response.json()
                if not visits:
                    # Break the loop if no more visits are returned
                    break
                all_visits.extend(visits)
                offset += limit  # Increase the offset for the next batch
            else:
                raise Exception(f"Error fetching data: {response.status_code} - {response.text}")

        return all_visits

    def write_data(data, extraction, schema, db_conn):
        import json

        import pandas as pd
        from sqlalchemy import MetaData, Table, create_engine
        from sqlalchemy.orm import sessionmaker

        if not data:
            print("No data to write!")
            return None

        select_columns = [
            "idVisit",
            "visitorId",
            "serverDate",
            "serverTimestamp",
            "userId",
            "visitorType",
            "referrerType",
            "referrerTypeName",
            "referrerName",
            "referrerKeyword",
            "referrerKeywordPosition",
            "referrerUrl",
            "referrerSearchEngineUrl",
            "referrerSearchEngineIcon",
            "referrerSocialNetworkUrl",
            "referrerSocialNetworkIcon",
            "deviceType",
            "deviceBrand",
            "deviceModel",
            "operatingSystemName",
            "continent",
            "country",
            "countryCode",
            "region",
            "regionCode",
            "city",
            "actionDetails",
        ]

        df = pd.DataFrame(data)

        df = df[select_columns]

        df_exploded = df.explode("actionDetails").reset_index(drop=True)

        df_normalized = pd.json_normalize(df_exploded["actionDetails"])

        df_normalized = df_normalized[["url", "pageIdAction", "timeSpent", "timestamp"]]

        df = df_exploded.drop("actionDetails", axis=1).join(df_normalized)

        def treat_complex_columns(col_value):
            if isinstance(col_value, (dict, list)):
                return json.dumps(col_value, ensure_ascii=False)
            return col_value

        print("##############################################################")
        pd.set_option("display.max_columns", None)
        pd.set_option("display.max_colwidth", 100)
        print(df)
        print("##############################################################")

        db_host = db_conn["pg_host"]
        db_port = db_conn["pg_port"]
        db_user = db_conn["pg_user"]
        db_pw = db_conn["pg_password"]
        db_db = db_conn["pg_db"]

        connection_string = f"postgresql://{db_user}:{db_pw}@{db_host}:{db_port}/{db_db}"
        engine = create_engine(connection_string)

        df = df.applymap(treat_complex_columns)

        sess = sessionmaker(bind=engine)
        session = sess()
        metadata = MetaData(schema=schema)

        table = Table(extraction, metadata, autoload_with=engine)

        to_delete = [str(x) for x in df["serverDate"].unique()]
        delete_query = table.delete().where(table.c.serverDate.in_(to_delete))

        result = session.execute(delete_query)
        session.commit()

        print(f"Rows deleted: {result.rowcount}")

        session.close()

        df.to_sql(
            name=extraction,
            con=engine,
            schema=schema,
            if_exists="append",
            index=False,
        )

        print(f"DataFrame written to {schema}.{extraction}.")

    extract_data_task = PythonVirtualenvOperator(
        task_id="extract_data",
        python_callable=fetch_visits_details,
        requirements=[],
        op_args=[api_url, site_id, api_token, start_date, end_date, limit],
        system_site_packages=True,
    )

    write_data_task = PythonVirtualenvOperator(
        task_id="write_data",
        python_callable=write_data,
        requirements=["pandas", "sqlalchemy"],
        op_args=[
            "{{ ti.xcom_pull(task_ids='extract_data') }}",
            "matomo_detailed_visits",
            "raw",
            destination_db_conn,
        ],
        system_site_packages=True,
        outlets=[Dataset("bronze_matomo_detailed_visits")],
    )

    extract_data_task >> write_data_task


data_ingestion_matomo_detailed_visits = data_ingestion_matomo_detailed_visits()
