# Repositório de DAGs do Brasil Participativo
## OnBoarding
Para inicializar nosso projeto siga nosso tutorial de [instalação](https://gitlab.com/lappis-unb/decidimbr/airflow-envs/-/wikis/home)!

## Contribuição

Para apoiar o projeto, leia o nosso guia de [contribuição](https://gitlab.com/lappis-unb/decidimbr/airflow-dags/-/blob/fix/documentacao/CONTRIBUTING.md?ref_type=heads).

E para entender um pouco mais, leia também nosso  [fluxo de desenvolvimento da engenharia de dados](https://gitlab.com/lappis-unb/decidimbr/ecossistemasl/-/wikis/estrutura/Engenharia-de-Dados/Fluxo%20de%20Desenvolvimento).





## Documentação da arquitetura

Para ler os detalhes sobre a arquitetura de dados acesse a [documentação de arquitetura.](https://gitlab.com/lappis-unb/decidimbr/ecossistemasl/-/wikis/estrutura/Servi%C3%A7os-de-Dados/Arquitetura-de-dados)

## Estrutura de Repositório de DAGs Airflow

Este repositório contém DAGs (Directed Acyclic Graphs) do Apache Airflow organizados em diversas pastas para melhorar a clareza e manutenção do código. Abaixo está uma breve descrição de cada pasta:

## Data Lake:
  - Armazenamento centralizado e organizado de dados brutos, não processados. Contém DAGs responsáveis pela ingestão e armazenamento de dados brutos provenientes de diferentes fontes.

## Data Warehouse:
  - Contém DAGs relacionadas à transformação e carregamento de dados no Data Warehouse. Inclui DAGs que realizam ETL (Extract, Transform, Load) para levar dados do Data Lake ao Data Warehouse, agregando valor e estrutura.

## Notificações:
  - Responsável por DAGs que enviam notificações via Telegram para a equipe de moderação. Pode incluir DAGs que alertam sobre eventos críticos ou informações importantes que requerem a atenção da equipe.

## Plugins:
  - Local destinado à extensão e customização do Airflow através de plugins. Pode incluir módulos adicionais, operadores personalizados, ganchos ou conexões específicas que ampliam as capacidades do Airflow.

## Processes_Confs:
  - Armazena arquivos YAML para configurar DAGs com diferentes parametrizações. Os arquivos YAML contêm configurações específicas para a geração dinâmica de DAGs, permitindo uma maior flexibilidade.


### Histórico de Versões

| Versão | Data | Descrição | Autor |
|--------|------|-----------|-------|
| 1.0 | 13/01/2024 | Criação do README.md | [Paulo Gonçalves](https://gitlab.com/PauloGoncalvesLima) |
| 2.0 | 01/02/2024 | Adição de guias  | [Laura Pinos](https://gitlab.com/laurapinos) |
