# Documentação da DAG para gerar o relatório de textos participativos

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Índice**

- [Documentação da DAG para gerar o relatório de textos participativos](#documenta%C3%A7%C3%A3o-da-dag-para-gerar-o-relat%C3%B3rio-de-textos-participativos)
    - [Introdução](#introdu%C3%A7%C3%A3o)
    - [Informações Gerais](#informa%C3%A7%C3%B5es-gerais)
    - [Configuração da DAG](#configura%C3%A7%C3%A3o-da-dag)
    - [Descrição das Tarefas](#descri%C3%A7%C3%A3o-das-tarefas)
    - [Funções auxiliares](#fun%C3%A7%C3%B5es-auxiliares)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Introdução

Essa DAG faz uma busca na api do Decidim para extrair dados de propostas gerados a partir de um texto participativo, e seus respectivos comentários. Insere tudo em um relatório HTML formatado, converte para PDF, e finalmente, envia para o email do gestor.

## Informações Gerais

- **Nome da DAG:** generate_report_participatory_texts
- **Descrição:** DAG que gera relatório e envia por email.
- **Agendamento:** A DAG não tem agendamento
- **Autor:** Joyce e Thaís
- **Versão:** 1.0
- **Data de Criação:** 23/02/2024

## Configuração da DAG

Antes de executar a DAG, certifique-se de configurar corretamente os seguintes parâmetros:

1. **Configuração de ambiente:** Subir o airflow.
    - **Passo 1:** Rodar o docker do repositório [airflow-docker](https://gitlab.com/lappis-unb/decidimbr/airflow-docker)
        - **airflow** O airflow se encontra no: <http://localhost:8080>

2. **Rodar as tarefas:** Testando a dag.
    - **Passo 1:** Rodar o docker do repositório [airflow-docker](https://gitlab.com/lappis-unb/decidimbr/airflow-docker)
        - **airflow** O airflow se encontra no: <http://localhost:8080>

    - **Passo 2:** Para rodar via terminal entre no container docker: ´docker exec -ti airflow-docker-airflow-webserver-1 bash´

    - **Passo 3:** Para rodar a Dag: ´airflow dags test dag_generate_report´

    - **Passo 4:** Para rodar uma tarefa específica: ´airflow tasks test dag_generate_report <nome_da_task>´

## Descrição das Tarefas

- **Nome:** get_component_data
- **Descrição:** Extrai dados dos componentes via api do Decidim e trata-os.
- **Dependências:** GraphQLHook.
- **Task inicial:** Sim.
- **Task final:** Não.

---

- **Nome:** filter_component_data
- **Descrição:** Aplica o filtro baseado na data.
- **Dependências:** Nenhuma.
- **Task inicial:** Não.
- **Task final:** Não.

---

- **Nome:** generate_data
- **Descrição:** Gera o relatório.
- **Dependências:** Nenhuma.
- **Task inicial:** Não.
- **Task final:** Não.

---

- **Nome:** send_report_email
- **Descrição:**  Envia o relatório por email.
- **Dependências:** Nenhuma.
- **Task inicial:** Não.
- **Task final:** Sim.

## Funções auxiliares

- **Nome:** _get_participatory_texts_data
- **Descrição:** Extrai dados dos componentes via api do Decidim e trata-os.
- **Parâmetros:** ID do componente, data inicial e data final.
- **Retorno:** Dicionário de dados.
- **Dependências:** GraphQLHook

---

- **Nome:** apply_filter_data
- **Descrição:** Aplica o filtro baseado na data.
- **Parâmetros:** Data do componente.
- **Retorno:** Dados filtrados.
- **Dependências:** Nenhuma.

---

- **Nome:** _generate_report
- **Descrição:** Gera relatório em pdf.
- **Parâmetros:**  Dados já filtrados.
- **Retorno:** PDF.
- **Dependências:** plugins.

---

- **Nome:** send_email_with_pdf
- **Descrição:** Envia o PDF por e-mail.
- **Parâmetros:**  E-mail, corpo de e-mail, pdf, assunto de e-mail.
- **Retorno:** PDF em Bytes.
