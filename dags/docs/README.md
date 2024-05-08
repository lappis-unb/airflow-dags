# Documentação
<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Índice**

- [Documentação](#documenta%C3%A7%C3%A3o)
    - [Documentação de DAGs](#documenta%C3%A7%C3%A3o-de-dags)
    - [Documentação de OnBoarding](#documenta%C3%A7%C3%A3o-de-onboarding)
    - [Informações Auxiliares](#informa%C3%A7%C3%B5es-auxiliares)
    - [Volumes](#volumes)
    - [Criando uma DAG local](#criando-uma-dag-local)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Documentação de DAGs

A pasta **docs** possui a documentação das Dags criadas e mantidas nesse repositório além de toda e qualquer documentação de suporte.

## Documentação de OnBoarding

Na pasta **docs** também se encontra a documentação do OnBoarding para inicialização do projeto.

### Como documentar a minha DAG?

Como critério de aceitação, todas as DAGs criadas precisam ser documentadas seguindo o template disponível em [dag-doc-template.md](docs/dag-doc-template.md).

## Informações Auxiliares

## Volumes

- Os arquivos do banco de dados estão em `./mnt/pgdata`.
- As DAGs devem estar em um diretório paralelo chamado `airflow-dags`.

## Criando uma DAG local

Vamos começar criando uma Dag de extração de propostas no MinIO

### Configuração da DAG

Antes de executar a DAG, certifique-se de configurar corretamente os seguintes parâmetros:

1. **Configuração de ambiente:** Subir o airflow e minIO.
    - **Passo 1:** Rodar o docker do repositório [airflow-environments](https://gitlab.com/lappis-unb/decidimbr/airflow-envs)
        - **airflow** O airflow se encontra no: <http://localHost:8080>
        - **MinIO** O MinIO se encontra no: <http://localHost:9001>

2. **Airflow:** Configuração do MinIO no Airflow.
    - **Passo 1:** Adicionar variáveis
        -ADMIN->VARIABLES
        - **Nome:** api_decidim
        - **Valor:** <https://lab-decide.dataprev.gov.br/api> ou <https://brasilparticipativo.presidencia.gov.br/api>

    - **Passo 2:** Criar uma conexão no Airflow para o MinIO
        - **Connection ID:** minio_connection_id
        - **Host:** <http://minio:9000>
        - **Schema:** daily-csv
        - **Login:** lappis
        - **Password:** lappisrocks

    - **Passo 3:** Criar uma conexão no Airflow para a API da plataforma Brasil Participativo
        - **Connection ID:** api_decidim
        - **Host:** <https://lab-decide.dataprev.gov.br/api> ou <https://brasilparticipativo.presidencia.gov.br/api>
        - **Login:** Seu email cadastrado na plataforma Brasil Participativo
        - **Password:** Sua senha cadastrada na plataforma Brasil Participativo

    - **Passo 4:** Criar outra conexão no Airflow para a API da plataforma Brasil Participativo
        - **Connection ID:** bp_conn
        - **Host:** <https://lab-decide.dataprev.gov.br/api> ou <https://brasilparticipativo.presidencia.gov.br/api>
        - **Login:** Seu email cadastrado na plataforma Brasil Participativo
        - **Password:** Sua senha cadastrada na plataforma Brasil Participativo

    - **Passo 5:** Criar outra conexão no Airflow para a API da plataforma Brasil Participativo
        - **Connection ID:** bp_conn_prod
        - **Host:** <https://lab-decide.dataprev.gov.br/api> ou <https://brasilparticipativo.presidencia.gov.br/api>
        - **Login:** Seu email cadastrado na plataforma Brasil Participativo
        - **Password:** Sua senha cadastrada na plataforma Brasil Participativo

    - **Passo 6:** Criar outra conexão no Airflow para o Matomo
        - **Connection ID:** matomo_conn
        - **Host:** <https://ew.dataprev.gov.br/>
        - **Login:** 18
        - **Password:** Senha do Matomo

        *OBS:  Você deve entrar em contato para solicitar a senha do Matomo*

    - **Passo 7:** Criar uma conexão no Airflow para o serviço de e-mail
        - **Connection ID:** gmail_smtp
        - **Connection Type:** SMTP
        - **Host:** smtp.gmail.com
        - **Login:** <servicosdados@gmail.com>
        - **Password:** <senha do e-mail>
        - **Port:** 587
        - **From Email:** <servicosdados@gmail.com>
        - **Disable SSL:** True

    - **Passo 8:** Abrir o MinIO e criar um bucket com o mesmo nome do schema (daily-csv)

3. **Rodar as tarefas:** Testando a dag.
    - **Passo 1:** Rodar o docker do repositório [airflow-environments](https://gitlab.com/lappis-unb/decidimbr/airflow-envs)
        - **airflow** O airflow se encontra no: <http://localHost:8080>
        - **MinIO** O MinIO se encontra no: <http://localHost:9001>
         **Passo 2:** Para rodar via terminal entre no container docker:
   ```docker exec -ti airflow-envs-airflow-webserver-1 bash```

         **Passo 3:** Para rodar a Dag: ```airflow dags test <nome_da_dag>```

         **Passo 4:** Para rodar uma tarefa específica: ```airflow tasks test <nome_da_dag> <nome_da_task>```

### Descrição das Tarefas

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

### Funções auxiliares

- **Nome:** save_to_minio
- **Descrição:** Faz a conexão com o MinIO e salva os dados
- **Parâmetros:** Dicionário de dados e nome do arquivo a ser salvo como CSV
- **Dependências:** BaseHook

### Desligando o Ambiente Airflow

Para desligar o ambiente, utilize:

```bash
docker-compose down
```

### Atualizando o Ambiente Docker

Se houver alterações no Docker-compose ou em dependências, execute:

```bash
docker-compose build
```

Caso as DAGs não estejam aparecendo reinicie sua máquina
