# Documentação da DAG X

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Introdução

Descrição da DAG, seus objetivos e escopo.

## Informações Gerais

- **Nome da DAG:** MinhaDAG
- **Descrição:** Breve descrição
- **Agendamento:** A DAG está agendada para ser executada ...
- **Autor:** Seu Nome
- **Versão:** 1.0
- **Data de Criação:** DD/MM/AAAA

## Configuração da DAG

Antes de executar a DAG, certifique-se de configurar corretamente os seguintes parâmetros:

1. **Configuração de ambiente:** Subir o airflow e minIO.
    - **Passo 1:** Rodar o docker do repositório [airflow-docker](https://gitlab.com/lappis-unb/decidimbr/airflow-docker)
        - **airflow** O airflow se encontra no: <http://localhost:8080>

2. **Rodar as tarefas:** Testando a dag.
    - **Passo 1:** Rodar o docker do repositório [airflow-docker](https://gitlab.com/lappis-unb/decidimbr/airflow-docker)
        - **airflow** O airflow se encontra no: <http://localhost:8080>

    - **Passo 2:** Para rodar via terminal entre no container docker: ´docker exec -ti airflow-envs-airflow-webserver-1 bash´

    - **Passo 3:** Para rodar a Dag: ´airflow dags test <nome_da_dag>´

    - **Passo 4:** Para rodar uma tarefa específica: ´airflow tasks test <nome_da_dag> <nome_da_task>´

3. **Configuração 3:** Descrição da configuração 3
   - **Passo 1:** ...
   - **Passo 2:** ...

## Descrição das Tarefas

- **Nome:** task_a
- **Descrição:** Descrição da tarefa.
- **Dependências:** Nenhuma.
- **Task inicial:** Sim ou Não
- **Task final:** Sim ou Não

- **Nome:** task_b
- **Descrição:** Descrição da tarefa.
- **Dependências:** Nenhuma
- **Task inicial:** Sim ou Não
- **Task final:** Sim ou Não

## Funções auxiliares

(Apagar caso não exista)

- **Nome:** nome_da_funçao
- **Descrição:** Descrição da função
- **Parâmetros:** (Apagar caso não exista)
- **Retorno:** (Apagar caso não exista)
- **Dependências:** Nenhuma
