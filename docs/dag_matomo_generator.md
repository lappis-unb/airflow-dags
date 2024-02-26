# Documentação da DAG matomo generator

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Introdução

DAG responsável por buscar dados analíticos de uma instância do Matomo e salvá-lo como arquivos CSV em um bucket do MinIO. Esta DAG destina-se para executar diariamente e buscar os dados para o dia específico de execução.

## Informações Gerais

- **Nome da DAG:** generate_extraction_dag
- **Descrição:** DAG que gera CSV do matomo para alimentar o MinIO
- **Agendamento:** todos os dias às 05h
- **Autor:** Nitai
- **Versão:** 1.0
- **Data de Criação:** 04/08/2023

- **Nome da DAG:** matomo_data_ingestion
- **Descrição:** DAG para ingerir dados do MinIO no banco de dados PostgreSQL
- **Agendamento:** todos os dias às 07h
- **Autor:** Nitai
- **Versão:** 1.0
- **Data de Criação:** 04/08/2023

## Configuração da DAG

Antes de executar a DAG, certifique-se de configurar corretamente os seguintes parâmetros:

1. **Configuração de ambiente:** Subir o airflow e minIO.
    - **Passo 1:** Rodar o docker do repositório [airflow-environments](https://gitlab.com/lappis-unb/decidimbr/airflow-envs)
        - **airflow** O airflow se encontra no: <http://localhost:8080>
        - **MinIO** O MinIO se encontra no: <http://localhost:9001>

2. **Airflow:** Configuração do MinIO no Airflow.
    - **Passo 1:** Adicionar variáveis
        - **Nome:** api_decidim
        - **Valor:** <https://lab-decide.dataprev.gov.br/api>

    - **Passo 2:** Criar uma conexão para o MinIO no Airflow
        - **Nome:** minio_connection_id
        - **host:** <http://minio:9000>
        - **schema:** matomo-daily-csv
        - **login:** lappis
        - **password:** lappisrocks

    - **Passo 3:** Criar uma conexão para o Matomo no Airflow
        - **Nome:** matomo_connection_id
        - **host:** <https://ew.dataprev.gov.br/>
        - **login:** 18
        - **password:** [token de acesso produção, deve ser solicitado à equipe]

    - **Passo 3.1:** Replicar a conexão para o Matomo no Airflow, com um nome diferente
        - **Nome:** matomo_conn
        - **host:** Mesmas configurações que anterior.
        - **login:** Mesmas configurações que anterior.
        - **password:** Mesmas configurações que anterior.  

    - **Passo 4:** Abrir o MinIO e criar um bucket com o mesmo nome do schema (matomo-daily-csv)

3. **Rodar as tarefas:** Testando a dag.
    - **Passo 1:** Rodar o docker do repositório [airflow-environments](https://gitlab.com/lappis-unb/decidimbr/airflow-envs)
        - **airflow** O airflow se encontra no: <http://localhost:8080>
        - **MinIO** O MinIO se encontra no: <http://localhost:9001>

    - **Passo 2:** Para rodar via terminal entre no container docker: ´docker exec -ti airflow-envs-airflow-webserver-1 bash´

    - **Passo 3:** Para rodar a Dag: ´airflow dags test generate_extraction_dag´

    - **Passo 4:** Para rodar uma tarefa específica: ´airflow tasks test generate_extraction_dag <nome_da_task>´

## Descrição das Tarefas

- **Nome:** fetch_data
- **Descrição:** Salva os dados no MinIO
- **Dependências:** save_to_minio, get_matomo_data
- **Task inicial:** Não
- **Task final:** Não

- **Nome:** ingest_data
- **Descrição:** Faz ingestão dos dados do MinIO no PostgreSQL
- **Dependências:** _ingest_into_postgres
- **Task inicial:** Não
- **Task final:** Não

## Funções auxiliares

- **Nome:** _create_s3_client
- **Descrição:** Faz o login no MinIO
- **Dependências:** BaseHook

- **Nome:** _generate_s3_filename
- **Descrição:** Retorna o nome do CSV
- **Parâmetros:** modulo, método, data de execução
- **Dependências:** Nenhuma

- **Nome:** add_temporal_columns
- **Descrição:** Adiciona colunas temporais ao DataFrame com base na data de execução
- **Parâmetros:** O DataFrame original sem colunas temporais
- **Dependências:** pandas
