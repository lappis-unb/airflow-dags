{{ config(
    materialized='table',
    schema='dbt',
    meta={
        "datasets_trigger": ["user_country_get_country"]
    }
) }}


SELECT 
    SPLIT_PART(SUBSTRING(url FROM LENGTH('pageUrl==https://brasilparticipativo.presidencia.gov.br/') + 1), '/', 4) AS DSC_processo_id,
    "label" AS DSC_PAIS, 
    code AS CODIGO_PAIS, 
    logo AS LOGO_PAIS, 
    nb_visits AS NUM_VISITAS, 
    nb_actions AS NUM_ACOES, 
    max_actions AS NUM_MAXIMO_ACOES, 
    sum_visit_length AS NUM_DURACAO_TOTAL_VISITAS, 
    nb_visits_converted AS NUM_VISITAS_CONVERTIDAS, 
    sum_daily_nb_uniq_visitors AS NUM_TOTAL_VISITANTES_UNICOS_DIARIOS, 
    sum_daily_nb_users AS NUM_TOTAL_USUARIOS_DIARIOS, 
    bounce_count AS NUM_CONTAGEM_REJEICOES, 
    "date" AS DAT_REF, 
    SUBSTRING(url FROM LENGTH('pageUrl==') + 1) AS URL
FROM 
    {{ source('raw', 'user_country_get_country') }}
where period = 'month'   
    ;
