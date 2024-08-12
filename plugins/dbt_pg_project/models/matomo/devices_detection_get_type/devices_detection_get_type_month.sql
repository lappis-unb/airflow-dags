{{ config(
    materialized='table',
    schema='dbt',
    meta={
        "datasets_trigger": ["devices_detection_get_type"]
    }
) }}


SELECT 
	SPLIT_PART(SUBSTRING(url FROM LENGTH('pageUrl==https://brasilparticipativo.presidencia.gov.br/') + 1), '/', 4) AS DSC_processo_URL_ID,
    "label" AS DSC_DISPOSITIVO,
    nb_uniq_visitors AS NUM_VISITANTES_UNICOS,
    nb_visits AS NUM_TOTAL_VISITAS,
    nb_actions AS NUM_TOTAL_ACOES,
    nb_users AS NUM_TOTAL_USUARIOS,
    max_actions AS NUM_MAX_ACOES_POR_VISITA,
    sum_visit_length AS NUM_DURACAO_TOTAL_VISITAS,
    bounce_count AS NUM_TOTAL_REJEICOES,
    nb_visits_converted AS NUM_VISITAS_CONVERTIDAS,
    "space" AS DSC_ESPACO_PARTICIPATIVO,
    "date" AS DAT_REF,
    "period" AS DSC_PERIODO,
    sum_daily_nb_users AS NUM_TOTAL_USUARIOS_DIARIOS,
    sum_daily_nb_uniq_visitors AS NUM_VISITANTES_UNICOS_DIARIOS,
    SUBSTRING(url FROM LENGTH('pageUrl==') + 1) AS DSC_URL,
FROM 
    {{ source('raw', 'devices_detection_get_type') }}
where period = 'month'
    ;
