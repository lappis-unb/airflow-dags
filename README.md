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

### Processes_Confs

- Armazena arquivos YAML para configurar DAGs com diferentes parametrizações. Os arquivos YAML contêm configurações específicas para a geração dinâmica de DAGs, permitindo uma maior flexibilidade.
