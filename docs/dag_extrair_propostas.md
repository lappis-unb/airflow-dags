# Documentação da DAG extrair proposta para o MinIO

## Introdução

Esta Dag faz uma busca na api do decidim para pegar dados referentes as propostas e os dados relacionados a elas. Gera CSV's com essas informações e envia para o MinIO, gerando dados Bronze.

## Informações Gerais

- **Nome da DAG:** decidim_data_extraction
- **Descrição:** DAG que gera CSV para alimentar o MinIO
- **Autor:** Thaís e Paulo
- **Versão:** 1.0
- **Data de Criação:** 14/01/2024

## Configuração da DAG

Antes de executar a DAG, certifique-se de configurar corretamente os seguintes parâmetros:

1. **Configuração de ambiente:** Subir o airflow e minIO.
    - **Passo 1:** Rodar o docker do repositório [airflow-environments](https://gitlab.com/lappis-unb/decidimbr/airflow-envs)
        - **airflow** O airflow se encontra no: http://localhost:8080
        - **MinIO** O MinIO se encontra no: http://localhost:9001

2. **Airflow:** Configuração do MinIO no Airflow.
    - **Passo 1:** Adicionar variáveis
        - **Nome:** api_decidim
        - **Valor:** https://lab-decide.dataprev.gov.br/api

    - **Passo 2:** Criar uma conexão para o MinIO no Airflow
        - **Nome:** minio_connection_id 
        - **host:** http://minio:9000
        - **schema:** daily-csv
        - **login:** lappis
        - **password:** lappisrocks
    
    - **Passo 3:** Abrir o MinIO e criar um bucket com o mesmo nome do schema (daily-csv)

3. **Rodar as tarefas:** Testando a dag.
    - **Passo 1:** Rodar o docker do repositório [airflow-environments](https://gitlab.com/lappis-unb/decidimbr/airflow-envs)
        - **airflow** O airflow se encontra no: http://localhost:8080
        - **MinIO** O MinIO se encontra no: http://localhost:9001

    - **Passo 2:** Para rodar via terminal entre no container docker: ´docker exec -ti airflow-envs-airflow-webserver-1 bash´

    - **Passo 3:** Para rodar a Dag: ´airflow dags test decidim_data_extraction´

    - **Passo 4:** Para rodar uma tarefa específica: ´airflow tasks test decidim_data_extraction <nome_da_task>´


## Descrição das Tarefas

- **Nome:** get_propolsas_components_ids
- **Descrição:** Retorna todos os ids de componentes
- **Dependências:** GraphQLHook
- **Task inicial:** Não
- **Task final:** Não


- **Nome:** get_proposals
- **Descrição:** Faz requisição de propostas na API do decidim, trata esses dados e salva no MinIO
- **Dependências:** get_propolsas_components_ids e ProposalsHook
- **Task inicial:** Não
- **Task final:** Não


- **Nome:** get_proposals_commments
- **Descrição:** Faz requisição de comentários de propostas na API do decidim, trata esses dados e salva no MinIO
- **Dependências:** get_propolsas_components_ids e ProposalsHook
- **Task inicial:** Não
- **Task final:** Não


## Funções auxiliares

- **Nome:** save_to_minio
- **Descrição:** Faz a conexão com o MinIO e salva os dados
- **Parâmetros:** Dicionário de dados e nome do arquivo a ser salvo como CSV
- **Dependências:** BaseHook

## Agendamento

A DAG está agendada para ser executada todos os dias às 23h.
