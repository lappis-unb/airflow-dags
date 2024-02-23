# Documentação da DAG para gerar o relatório de propostas

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Introdução

Essa DAG faz uma busca na api do Decidim e do Matomo para extrair dados sobre propostas e acessos em suas respectivas páginas.Trata esses dados, gera gráficos, tabelas, insere tudo em um relatório HTML formatado, converte para PDF, e finalmente, envia para o email do gestor.

## Informações Gerais

- **Nome da DAG:** dag_generate_report
- **Descrição:** DAG que gera relatório de propostas e envia por email.
- **Agendamento:** A DAG não tem agendamento
- **Autor:** Joyce e Paulo
- **Versão:** 1.0
- **Data de Criação:** 21/02/2024

## Configuração da DAG

- **Nome:** _get_components_url
- **Descrição:** Gera a url dos componentes.
- **Parâmetros:** ID do componente
- **Retorno:** Url do componente
- **Dependências:** ComponentBaseHook
Antes de executar a DAG, certifique-se de configurar corretamente os seguintes parâmetros:

1. **Configuração de ambiente:** Subir o airflow.
    - **Passo 1:** Rodar o docker do repositório [airflow-environments](https://gitlab.com/lappis-unb/decidimbr/airflow-envs)
        - **airflow** O airflow se encontra no: <http://localhost:8080>

2. **Rodar as tarefas:** Testando a dag.
    - **Passo 1:** Rodar o docker do repositório [airflow-environments](https://gitlab.com/lappis-unb/decidimbr/airflow-envs)
        - **airflow** O airflow se encontra no: <http://localhost:8080>

    - **Passo 2:** Para rodar via terminal entre no container docker: ´docker exec -ti airflow-envs-airflow-webserver-1 bash´

    - **Passo 3:** Para rodar a Dag: ´airflow dags test dag_generate_report´

    - **Passo 4:** Para rodar uma tarefa específica: ´airflow tasks test dag_generate_report <nome_da_task>´

## Descrição das Tarefas

- **Nome:** get_components_url
- **Descrição:** Gera urls dos componentes.
- **Dependências:** ComponentBaseHook.
- **Task inicial:** Sim
- **Task final:** Não

- **Nome:** get_component_data
- **Descrição:** Extrai dados dos componentes via api do Decidim.
- **Dependências:** GraphQLHook
- **Task inicial:** Sim
- **Task final:** Não

- **Nome:** get_matomo_VisitsSummary_get
- **Descrição:** Extrai um resumo dos dados de visitas dos componentes via api do Matomo.
- **Dependências:** get_components_url e BaseHook
- **Task inicial:** Não
- **Task final:** Não

- **Nome:** get_matomo_VisitFrequency_get
- **Descrição:** Extrai dados de frequência de visitas dos componentes via api do Matomo.
- **Dependências:** get_components_url e BaseHook
- **Task inicial:** Não
- **Task final:** Não

- **Nome:** get_matomo_UserCountry_getRegion
- **Descrição:** Extrai dados geográficos de visitas dos componentes via api do Matomo.
- **Dependências:** get_components_url e BaseHook
- **Task inicial:** Não
- **Task final:** Não

- **Nome:** get_matomo_DevicesDetection_getType
- **Descrição:** Extrai dados de dispositivos de visitas dos componentes via api do Matomo.
- **Dependências:** get_components_url e BaseHook
- **Task inicial:** Não
- **Task final:** Não

- **Nome:** generate_data
- **Descrição:** Trata os dados extraídos e gera o relatório.
- **Dependências:** get_matomo_VisitsSummary_get, get_matomo_VisitFrequency_get, get_matomo_UserCountry_getRegion e get_matomo_DevicesDetection_getType
- **Task inicial:** Não
- **Task final:** Não

- **Nome:** send_report_email
- **Descrição:**  Envia o relatório por email.
- **Dependências:** MIMEApplication, MIMEMultipart, MIMEText e generate_data
- **Task inicial:** Não
- **Task final:** Sim

## Funções auxiliares

- **Nome:** _get_components_url
- **Descrição:** Gera a url dos componentes.
- **Parâmetros:** ID do componente
- **Retorno:** Url do componente
- **Dependências:** ComponentBaseHook

- **Nome:** _get_proposals_data
- **Descrição:** Extrai os dados de propostas da api do Decidim.
- **Parâmetros:** ID do componente, data de início, data de fim
- **Retorno:** Dados extraídos das propostas
- **Dependências:** GraphQLHook

- **Nome:** _get_matomo_data
- **Descrição:** Extrai os dados de acessos da api do Matomo.
- **Parâmetros:**  URL, data de início, data de fim, módulo e método
- **Retorno:** Dados de acesso extraídos
- **Dependências:** BaseHook

- **Nome:** _generate_report
- **Descrição:** Gera o relatório do PDF.
- **Parâmetros:**  Dados extraídos do Decidim, Dados extraídos do Matomo
- **Retorno:** PDF em Bytes

- **Nome:** send_email_with_pdf
- **Descrição:** Envia o relatório por email.
- **Parâmetros:**  Email, pdf em bytes, assunto e corpo do email
