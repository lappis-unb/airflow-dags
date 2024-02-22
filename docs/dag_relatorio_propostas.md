# Documentação da DAG para gerar o relatório

## Introdução

Essa DAG faz uma busca na api do Decidim e do Matomo para pegar dados sobre propostas e dados de acessos sobre a pagina da proposta. Trata os dados extraidos e gera gráficos e tabelas referentes aquela determinada proposta e finalmente gera um relatório em HTML e converte para PDF e envia para o email do gesto.

## Informações Gerais

- **Nome da DAG:** dag_generate_report
- **Descrição:** DAG que gera relatório de propostas e envia por email.
- **Agendamento:** A DAG não tem agendamento
- **Autor:** Joyce e Paulo
- **Versão:** 1.0
- **Data de Criação:** 21/02/2024

## Configuração da DAG

- **Nome:** _get_components_url
- **Descrição:** Gera a url dos componentes
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
- **Descrição:** Extrai dados de geográficos de visitas dos componentes via api do Matomo.
- **Dependências:** get_components_url e BaseHook
- **Task inicial:** Não
- **Task final:** Não

- **Nome:** get_matomo_DevicesDetection_getType
- **Descrição:** Extrai dados de dispositivos de visitas dos componentes via api do Matomo.
- **Dependências:** get_components_url e BaseHook
- **Task inicial:** Não
- **Task final:** Não

- **Nome:** generate_data
- **Descrição:** Trata os dados extraidos e gera o relatorio.
- **Dependências:** get_matomo_VisitsSummary_get, get_matomo_VisitFrequency_get, get_matomo_UserCountry_getRegion e get_matomo_DevicesDetection_getType
- **Task inicial:** Não
- **Task final:** Não

- **Nome:** send_report_email
- **Descrição:**  Envia o relatório por email
- **Dependências:** MIMEApplication, MIMEMultipart, MIMEText e generate_data
- **Task inicial:** Não
- **Task final:** Sim

## Funções auxiliares

_get_components_url
_get_proposals_data
_get_matomo_data
_generate_report
send_email_with_pdf

- **Nome:** _get_components_url
- **Descrição:** Gera a url dos componentes
- **Parâmetros:** ID do componente
- **Retorno:** Url do componente
- **Dependências:** ComponentBaseHook

- **Nome:** _get_proposals_data
- **Descrição:** Extrai os dados de propostas da api do Decidim
- **Parâmetros:** ID do componente, data de inicio, data de fim
- **Retorno:** Dados extraidos das propostas
- **Dependências:** GraphQLHook

- **Nome:** _get_matomo_data
- **Descrição:** Extrai os dados de acessos da api do Matomo
- **Parâmetros:**  URL, data de inicio, data de fim, modulo e metodo
- **Retorno:** Dados de acesso extraidos
- **Dependências:** BaseHook

- **Nome:** _generate_report
- **Descrição:** Gera o relatório do PDF
- **Parâmetros:**  Dados extraidos do Decidim, Dados extraidos do Matomo
- **Retorno:** PDF em Bytes

- **Nome:** send_email_with_pdf
- **Descrição:** Envia o relatorio por email
- **Parâmetros:**  Email, pdf em bytes, assunto e corpo do email
