{{ config(
    materialized='table',
    schema='dbt',
    meta={
        "datasets_trigger": ["user_country_get_region"]
    }
) }}


  SELECT 
        SPLIT_PART(SUBSTRING(url FROM LENGTH('pageUrl==https://brasilparticipativo.presidencia.gov.br/') + 1), '/', 4) AS PROCESSO_ID,
    country_name AS DSC_NOME_PAIS,
    region_name AS DSC_NOME_REGIAO,
    region AS DSC_ESTADO,
    nb_uniq_visitors AS NUM_VISITANTES_UNICOS,
    nb_visits AS NUM_TOTAL_VISITAS,
    nb_users AS NUM_TOTAL_USUARIOS,
    max_actions AS NUM_MAX_ACOES_POR_VISITA,
    sum_visit_length AS NUM_DURACAO_TOTAL_VISITAS,
    nb_actions AS NUM_TOTAL_ACOES,
    nb_visits_converted AS NUM_VISITAS_CONVERTIDAS,
    bounce_count AS NUM_TOTAL_REJEICOES,
    "date" AS DAT_REF,
    SUBSTRING(url FROM LENGTH('pageUrl==') + 1) AS DSC_URL
FROM 
    {{ source('raw', 'user_country_get_region') }}
where period = 'day'
    ;