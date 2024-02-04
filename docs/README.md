# Documentação de DAGs

A pasta **docs** possui a documentação das Dags criadas e mantidas nesse repositório além de toda e qualquer documentação de suporte.

# Documentação de OnBoarding
Na pasta **docs** também se encontra a documentação do OnBoarding para inicialização do projeto.

## Como documentar a minha DAG?

Como critério de aceitação, todas as DAGs criadas precisam ser documentadas seguindo o template disponível em [dag-doc-template.md](docs/dag-doc-template.md).

## Informações Auxiliares:

## Volumes

- Os arquivos do banco de dados estão em `./mnt/pgdata`.
- As DAGs devem estar em um diretório paralelo chamado `airflow-dags`.

##Criando uma DAG local
Vamos começar criando uma Dag de extração de propostas no MinIO
## Configuração da DAG

Antes de executar a DAG, certifique-se de configurar corretamente os seguintes parâmetros:

1. **Configuração de ambiente:** Subir o airflow e minIO.
    - **Passo 1:** Rodar o docker do repositório [airflow-environments](https://gitlab.com/lappis-unb/decidimbr/airflow-envs)
        - **airflow** O airflow se encontra no: http://localhost:8080
        - **MinIO** O MinIO se encontra no: http://localhost:9001

2. **Airflow:** Configuração do MinIO no Airflow.
    - **Passo 1:** Adicionar variáveis
        -ADMIN->VARIABLES 
        - **Nome:** api_decidim
        - **Valor:** https://lab-decide.dataprev.gov.br/api

    - **Passo 2:** Criar uma conexão no Airflow para o MinIO 
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
         **Passo 2:** Para rodar via terminal entre no container docker: 
   ```docker exec -ti airflow-envs-airflow-webserver-1 bash```

         **Passo 3:** Para rodar a Dag: ```airflow dags test <nome_da_dag>```

         **Passo 4:** Para rodar uma tarefa específica: ```airflow tasks test <nome_da_dag> <nome_da_task>```


## Descrição das Tarefas

- **Nome:** get_propolsas_components_ids
- **Descrição:** Retorna todos os ids de componentes
- **Dependências:** GraphQLHook
- **Task inicial:** Sim
- **Task final:** Não


- **Nome:** get_proposals
- **Descrição:** Faz requisição de propostas na API do decidim, trata esses dados e salva no MinIO
- **Dependências:** get_propolsas_components_ids e ProposalsHook
- **Task inicial:** Não
- **Task final:** Sim


- **Nome:** get_proposals_commments
- **Descrição:** Faz requisição de comentários de propostas na API do decidim, trata esses dados e salva no MinIO
- **Dependências:** get_propolsas_components_ids e ProposalsHook
- **Task inicial:** Não
- **Task final:** Sim


## Funções auxiliares

- **Nome:** save_to_minio
- **Descrição:** Faz a conexão com o MinIO e salva os dados
- **Parâmetros:** Dicionário de dados e nome do arquivo a ser salvo como CSV
- **Dependências:** BaseHook

## Desligando o Ambiente Airflow

Para desligar o ambiente, utilize:
```bash
docker-compose down
```
## Atualizando o Ambiente Docker
Se houver alterações no Docker-compose ou em dependências, execute:

```bash
docker-compose build
```
Caso as DAGs não estejam aparecendo reinicie sua máquina
