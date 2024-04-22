# Documentação da DAG notify new proposals

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Introdução

DAG que envia uma mensagem via telegram para avisar que novas propostas foram adicionados no Brasil Participativo.

## Informações Gerais

- **Nome da DAG:** notify_new_proposals
- **Agendamento:** A cada 3 minutos
- **Autor:** Paulo e Thaís
- **Versão:** 1.0
- **Data de Criação:** 27/08/2023

## Configuração da DAG

Antes de executar a DAG, certifique-se de configurar corretamente os seguintes parâmetros:

1. **Configuração de ambiente:** Subir o projeto.
    - **Passo 1:** Rodar o docker do repositório [airflow-docker](https://gitlab.com/lappis-unb/decidimbr/airflow-docker)
        - **airflow** O airflow se encontra no: <http://localhost:8080>

2. **Configuração do Airflow:**
    - **Passo 1:** Adicionar variáveis
        - **Nome:** api_decidim
        - **Valor:** <https://lab-decide.dataprev.gov.br/api>

    - **Passo 2:** Adicionar conexão
        - **Connection Id:** telegram_decidim
        - **Connection Type:** HTTP
        - **Host:** (adicionar o id do chatbot)
        - **schema:** (adicionar o id do canal/tópico)
        - **password:** (adicionar o token do chatbot)

    - **Observações:**
        - **token:** Para ter o token de acesso fale com o @BotFather caso seja um bot criado por você, caso contrario falar com o responsável pelo chat.
        - **configurações do chatbot:** para encontrar as configurações do chatbot acesse: ´<https://api.telegram.org/bot(TOKEN)/getUpdates´>

3. **Rodar as tarefas:** Testando a dag.
    - **Passo 1:** Rodar o docker do repositório [airflow-docker](https://gitlab.com/lappis-unb/decidimbr/airflow-docker)
        - **airflow** O airflow se encontra no: <http://localhost:8080>

    - **Passo 2:** Para rodar via terminal entre no container docker: ´docker exec -ti airflow-docker-airflow-webserver-1 bash´

    - **Passo 3:** Para rodar a Dag: ´airflow dags test <nome_da_dag>´

    - **Passo 4:** Para rodar uma tarefa específica: ´airflow tasks test decidim_data_extraction <nome_da_task>´

## Descrição das Tarefas

- **Nome:** get_update_date
- **Descrição:** Recupera data de atualização da última proposta
- **Dependências:** Nenhuma
- **Task inicial:** Sim
- **Task final:** Não

---

- **Nome:** get_proposals
- **Descrição:** Faz requisição de propostas na API do decidim
- **Dependências:** update_date
- **Task inicial:** Não
- **Task final:** Não

---

- **Nome:** mount_telegram_messages
- **Descrição:** Seleciona propostas novas e cria uma mensagem para ser enviada via telegram
- **Dependências:** get_proposals
- **Task inicial:** Não
- **Task final:** Não

---

- **Nome:** check_if_new_proposals
- **Descrição:** Escolhe o fluxo de tarefas caso tenha ou não novas propostas
- **Dependências:** mount_telegram_messages
- **Task inicial:** Não
- **Task final:** Não

---

- **Nome:** send_telegram_messages
- **Descrição:** Envia a mensagem para o telegram
- **Dependências:** mount_telegram_messages, check_if_new_proposals
- **Task inicial:** Não
- **Task final:** Não

---

- **Nome:** save_update_date
- **Descrição:** Adiciona a data de atualização da última proposta na variável geral
- **Dependências:** send_telegram_messages, mount_telegram_messages
- **Task inicial:** Não
- **Task final:** Sim

## Funções auxiliares

- **Nome:** read_yaml_files_from_directory
- **Descrição:** Lê as configurações salvas nos arquivos yaml do projeto e gera a Dag utilizando essas configurações
- **Dependências:** DecidimNotifierDAGGenerator
