{{ config(
    materialized='table',
    schema='dbt',
    meta={
        "datasets_trigger": ["visits_summary_get"]
    }
) }}

SELECT 
    SPLIT_PART(SUBSTRING(url FROM LENGTH('pageUrl==https://brasilparticipativo.presidencia.gov.br/') + 1), '/', 4) AS DSC_ID_PROCESSO,
    nb_uniq_visitors AS NUM_VISITANTES_UNICOS,
    nb_users AS NUM_TOTAL_USUARIOS,
    nb_visits AS NUM_TOTAL_VISITAS,
    nb_actions AS NUM_TOTAL_ACOES,
    sum_visit_length AS NUM_DURACAO_TOTAL_VISITAS,
    max_actions AS NUM_MAX_ACOES_POR_VISITA,
    nb_actions_per_visit AS NUM_ACOES_POR_VISITA,
    nb_visits_converted AS NUM_VISITAS_CONVERTIDAS,
    bounce_count AS NUM_TOTAL_REJEICOES,
    bounce_rate AS NUM_TAXA_REJEICAO,
    avg_time_on_site AS NUM_TEMPO_MEDIO_NO_SITE,
    space AS DSC_ESPACO_PARTICIPATIVO,
    date AS DAT_REF_VISITA,
    SUBSTRING(url FROM LENGTH('pageUrl==') + 1) AS DSC_URL_PAGINA
FROM
    {{ source('raw', 'visits_summary') }}
WHERE
    period = 'month'
