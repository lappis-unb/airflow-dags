# Repositório de DAGs do Brasil Participativo

Este repositório contém DAGs (Directed Acyclic Graphs) do Apache Airflow. As DAGs mantidas aqui são para apoiar na arquitetura dos dados e para auxiliar na automatização e apoio ao uso da plataforma brasil Participativo.

Sobre o projeto [Brasil Participativo](https://brasilparticipativo.presidencia.gov.br/processes/brasilparticipativo/f/26/posts/99)

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Índice**

- [Repositório de DAGs do Brasil Participativo](#reposit%C3%B3rio-de-dags-do-brasil-participativo)
    - [Pré-requisitos](#pr%C3%A9-requisitos)
    - [Instalação](#instala%C3%A7%C3%A3o)
    - [Documentação Auxiliar](#documenta%C3%A7%C3%A3o-auxiliar)
    - [Estrutura de Repositório](#estrutura-de-reposit%C3%B3rio)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Pré-requisitos

- [Docker](https://www.docker.com)
- [Docker Compose](https://docs.docker.com/compose/)

## Instalação

No repositório **[airflow-docker](https://gitlab.com/lappis-unb/decidimbr/airflow-docker)** estão os códigos e instruções da instalação e configuração do ambiente Airflow utilizado pelos desenvolvedores da Plataforma Brasil Participativo.

## Documentação Auxiliar

Na [Wiki do projeto](https://gitlab.com/groups/lappis-unb/decidimbr/servicos-de-dados/-/wikis/home) você encontra diagramas e informações sobre a arquitetura utilizada nesse projeto, fluxo de dados e outras informações importantes para colaborar.

## Estrutura de Repositório

### Data Lake

- Armazenamento centralizado e organizado de dados brutos, não processados. Contém DAGs responsáveis pela ingestão e armazenamento de dados brutos provenientes de diferentes fontes.

### Data Warehouse

- Contém DAGs relacionadas à transformação e carregamento de dados no Data Warehouse. Inclui DAGs que realizam ETL (Extract, Transform, Load) para levar dados do Data Lake ao Data Warehouse, agregando valor e estrutura.

### Notificações

- Responsável por DAGs que enviam notificações via Telegram para a equipe de moderação. Pode incluir DAGs que alertam sobre eventos críticos ou informações importantes que requerem a atenção da equipe.

### Plugins

- Local destinado à extensão e customização do Airflow através de plugins. Pode incluir módulos adicionais, operadores personalizados, ganchos ou conexões específicas que ampliam as capacidades do Airflow.

#### dbt

- Dentro do diretório plugins, nós temos um projeto dbt, chamado `dbt_pg_project`. Nesse projeto iremos adicionar todos os processos de tratamento de dados e de construção das camadas do nosso lakehouse.
- Para utilizar esse projeto, é necessário fazer o setup de duas variáveis de ambiente, `DBT_POSTGRES_USER` e `DBT_POSTGRES_PASSWORD`, para acesso ao banco postgres de interesse. Recomendamos criar um arquivo `.env` dentro do diretório `plugins/dbt_pg_project/.env` com a seguinte estrutura:

```bash
export DBT_POSTGRES_USER=<USUARIO>
export DBT_POSTGRES_PASSWORD=<SENHA>
```

- Ao iniciar uma sessão basta executar `source .env` (adicionalmente, podemos adicionar o setup dessas variáveis de ambiente no `.bashrc`). Para testar o funcionamento do projeto, podemos executar `dbt debug`. Todos os comandos aqui descritos devem ser executados da pasta `plugins/dbt_pg_project`.

- Nós temos duas automações que nos ajudam a orquestrar o projeto dbt no Airflow: (1) processo de CI/CD que irá compilar o projeto dbt e gerar o arquivo `dags/dbt/dbt_dag.py` e um processo de git sync que irá fazer a cópia dos arquivos de dags e plugins para a instância do Airflow no K8S. Com isso, o commit de alterações no projeto dbt em branches específicas é o suficiente para fazer o deploy do novo projeto nos ambientes associados às branches.

### Processes_Confs

- Armazena arquivos YAML para configurar DAGs com diferentes parametrizações. Os arquivos YAML contêm configurações específicas para a geração dinâmica de DAGs, permitindo uma maior flexibilidade.
