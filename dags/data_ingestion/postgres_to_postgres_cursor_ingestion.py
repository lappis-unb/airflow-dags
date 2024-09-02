import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from airflow.datasets import Dataset
from airflow.decorators import dag
from airflow.operators.python import PythonVirtualenvOperator

doc_md_DAG = '''

## Documentação da DAG `data_ingestion_postgres_cursor_ingestion`

### Descrição

A DAG `data_ingestion_postgres_cursor_ingestion` é responsável pela extração de dados de várias tabelas em um banco de dados PostgreSQL, utilizando uma configuração específica para cada tabela armazenada em arquivos `.json`. Esta DAG realiza tanto ingestões completas quanto incrementais e armazena os dados extraídos em um banco de dados de destino especificado.

### Detalhes da DAG

- **ID da DAG**: `data_ingestion_postgres_cursor_ingestion`
- **Tags**: `ingestion`, `{nome_da_tabela}` (as tags incluem a identificação de cada tabela conforme configurada no arquivo JSON)
- **Proprietário**: `data`
- **Agendamento**: Diariamente às 04:00 UTC (`0 4 * * *`)
- **Data de início**: Configurada conforme especificado no arquivo JSON de cada tabela.
- **Catchup**: Configurado conforme especificado no arquivo JSON de cada tabela.
- **Concurrency**: 1 (apenas uma execução da DAG por vez)
- **Renderização de templates como objetos nativos**: Ativado (`True`)

### Argumentos Padrão

- **Retries**: 2 (número de tentativas em caso de falha)
- **Retry Delay**: 10 minutos (tempo de espera entre as tentativas)

### Configuração da Ingestão

Esta DAG processa múltiplas tabelas conforme descrito em arquivos `.json` localizados no diretório `./cursor_ingestions`. Cada arquivo JSON deve conter as seguintes informações:

- **name**: Nome da extração (utilizado na definição da DAG e nas tarefas).
- **extractions**: Dicionário contendo as extrações a serem realizadas, onde cada chave representa o nome de uma tabela e o valor é um dicionário com as seguintes informações:
  - `extraction_schema`: Esquema de origem da tabela.
  - `ingestion_type`: Tipo de ingestão (`full_refresh` ou `incremental`).
  - `incremental_filter`: Filtro utilizado para identificar os novos dados em ingestações incrementais.
  - `destination_schema`: Esquema de destino onde os dados serão armazenados.
- **catchup**: Define se a DAG deve realizar catchup.
- **start_date**: Data de início da DAG no formato `YYYY-MM-DD`.
- **origin_db_connection**: ID da conexão para o banco de dados de origem no Airflow.
- **destination_db_connection**: ID da conexão para o banco de dados de destino no Airflow.

### Estrutura da DAG

Para cada extração configurada, a DAG cria duas tarefas:

#### 1. `extract_data_{nome_da_tabela}`

**Operador**: `PythonVirtualenvOperator`

- **Descrição**: Extrai os dados de uma tabela específica do banco de dados de origem utilizando o tipo de ingestão especificado (com ou sem túnel SSH).
- **Parâmetros de Entrada**:
  - `extraction`: Nome da tabela a ser extraída.
  - `extraction_info`: Informações de configuração da extração, conforme descritas no arquivo JSON.
  - `db_conn_id`: ID da conexão do banco de dados de origem no Airflow.
  - `ssh_tunnel`: Indica se deve ser utilizado um túnel SSH (padrão `True`).
- **Bibliotecas Necessárias**: `pandas`, `sqlalchemy`, `sshtunnel`
- **Saída**: Retorna um DataFrame contendo os dados extraídos.

#### 2. `write_data_{nome_da_tabela}`

**Operador**: `PythonVirtualenvOperator`

- **Descrição**: Escreve os dados extraídos na tabela de destino especificada no banco de dados de destino.
- **Parâmetros de Entrada**:
  - `df`: DataFrame contendo os dados extraídos.
  - `extraction`: Nome da tabela a ser escrita.
  - `extraction_info`: Informações de configuração da extração, conforme descritas no arquivo JSON.
  - `db_conn_id`: ID da conexão do banco de dados de destino no Airflow.
- **Bibliotecas Necessárias**: `pandas`, `sqlalchemy`
- **Saída**: Escreve os dados no banco de dados de destino, no esquema e tabela especificados.

#### Conexões e Relacionamentos

- Cada par de tarefas (`extract_data_{nome_da_tabela}` e `write_data_{nome_da_tabela}`) está encadeado, ou seja, a tarefa de extração deve ser concluída com sucesso antes que a tarefa de escrita seja executada.

### Conexões com Datasets

- **Output Dataset**: Cada tabela extraída gera um `Dataset` correspondente, identificado pelo nome `bronze_{nome_da_tabela}`.

'''

if TYPE_CHECKING:
    from sshtunnel import SSHTunnelForwarder


default_args = {"owner": "data", "retries": 2, "retry_delay": timedelta(minutes=10)}

origin_schema = "public"
destination_schema = "raw"


for entry in os.scandir(Path(__file__).parent.joinpath("./cursor_ingestions")):
    if not entry.name.endswith(".json"):
        continue

    with open(entry.path) as file:
        dag_config = json.load(file)

    dag_name: str = dag_config["name"]
    extractions: dict = dag_config["extractions"]
    catchup: bool = dag_config["catchup"]
    start_date: str = datetime.strptime(dag_config["start_date"], "%Y-%m-%d")

    @dag(
        dag_id=f"{dag_name}_postgres_cursor_ingestion",
        default_args=default_args,
        schedule_interval="0 4 * * *",
        start_date=start_date,
        tags=["ingestion", dag_name],
        catchup=catchup,
        concurrency=1,
        render_template_as_native_obj=True,
        doc_md=doc_md_DAG,
    )
    def data_ingestion_postgres(origin_db_connection, destination_db_connection):
        def extract_data(extraction, extraction_info, db_conn_id, ssh_tunnel: bool = False):
            import pandas as pd
            from airflow.providers.postgres.hooks.postgres import PostgresHook
            from airflow.providers.ssh.hooks.ssh import SSHHook
            from sqlalchemy import create_engine

            ssh_conn = "ssh_tunnel_decidim"
            extraction_schema = extraction_info["extraction_schema"]

            if extraction_info["ingestion_type"] == "full_refresh":
                query = f"SELECT * FROM {extraction_schema}.{extraction}"
            elif extraction_info["ingestion_type"] == "incremental":
                incremental_filter = extraction_info["incremental_filter"]
                query = f"SELECT * FROM {extraction_schema}.{extraction} where {incremental_filter}"

            print(query)

            db = PostgresHook.get_connection(db_conn_id)
            if ssh_tunnel:
                with SSHHook(ssh_conn).get_tunnel(remote_host=db.host, remote_port=db.port) as tunnel:
                    tunnel: SSHTunnelForwarder
                    engine = create_engine(
                        f"postgresql://{db.login}:{db.password}@127.0.0.1:{tunnel.local_bind_port}/{db.schema}"
                    )
                    df = pd.read_sql(query, engine)
            else:
                connection_string = f"postgresql://{db.login}:{db.password}@{db.host}:{db.port}/{db.schema}"
                engine = create_engine(connection_string)
                df = pd.read_sql(query, engine)

            return df

        def write_data(df, extraction, extraction_info, db_conn_id):
            import json

            import pandas as pd
            from airflow.providers.postgres.hooks.postgres import PostgresHook
            from sqlalchemy import MetaData, Table
            from sqlalchemy.exc import NoSuchTableError
            from sqlalchemy.orm import sessionmaker

            def treat_complex_columns(col_value):
                if isinstance(col_value, (dict, list)):
                    return json.dumps(col_value, ensure_ascii=False)
                return col_value

            print("##############################################################")
            pd.set_option("display.max_columns", None)
            pd.set_option("display.max_colwidth", 100)
            print(df)
            print("##############################################################")

            if df.empty:
                print("No data to write!")
                return None

            engine = PostgresHook(db_conn_id).get_sqlalchemy_engine()
            df = df.applymap(treat_complex_columns)

            schema = extraction_info["destination_schema"]
            insertion_method = "append"

            sess = sessionmaker(bind=engine)
            session = sess()
            metadata = MetaData(schema=schema)
            try:
                table = Table(extraction, metadata, autoload_with=engine)

                if extraction_info["ingestion_type"] == "incremental":
                    ids_to_delete = [str(x) for x in df["id"].unique()]
                    delete_query = table.delete().where(table.c.id.in_(ids_to_delete))
                else:
                    delete_query = table.delete()

                result = session.execute(delete_query)
                session.commit()
                print(f"Rows deleted: {result.rowcount}")

            except NoSuchTableError:
                print("Table does not exists.")

            session.close()

            df.to_sql(
                name=extraction,
                con=engine,
                schema=schema,
                if_exists=insertion_method,
                index=False,
            )

            print(f"DataFrame written to {schema}.{extraction}.")

        for extraction, extraction_info in extractions.items():  # noqa: B023
            extract_data_task = PythonVirtualenvOperator(
                task_id=f"extract_data_{extraction}",
                python_callable=extract_data,
                requirements=["pandas", "sqlalchemy", "sshtunnel"],
                op_args=[extraction, extraction_info, origin_db_connection, True],
                system_site_packages=True,
            )

            write_data_task = PythonVirtualenvOperator(
                task_id=f"write_data_{extraction}",
                python_callable=write_data,
                requirements=["pandas", "sqlalchemy"],
                op_args=[
                    f"{{{{ ti.xcom_pull(task_ids='extract_data_{extraction}') }}}}",
                    extraction,
                    extraction_info,
                    destination_db_connection,
                ],
                system_site_packages=True,
                outlets=[Dataset(f"bronze_{extraction}")],
            )

            extract_data_task >> write_data_task

    data_ingestion_postgres(dag_config["origin_db_connection"], dag_config["destination_db_connection"])
