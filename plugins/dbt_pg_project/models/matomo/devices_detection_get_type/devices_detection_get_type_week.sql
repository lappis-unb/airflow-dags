{{ config(
    materialized='table',
    schema='dbt',
    meta={
        "datasets_trigger": ["devices_detection_get_type"]
    }
) }}



SELECT 
	
	space AS TIPO_ESPACO,
    nb_users AS NUM_TOTAL_USUARIOS,
    nb_visits AS NUM_TOTAL_VISITAS,
    nb_uniq_visitors AS NUM_VISITANTES_UNICOS,
    nb_visits_converted AS NUM_VISITAS_CONVERTIDAS,
    sum_visit_length AS NUM_DURACAO_TOTAL_VISITAS,
    max_actions AS NUM_MAX_ACOES_POR_VISITA,
    avg_time_on_site AS NUM_TEMPO_MEDIO_NO_SITE,
    bounce_count AS NUM_TOTAL_REJEICOES,
    bounce_rate AS NUM_TAXA_REJEICAO,
    nb_actions AS NUM_TOTAL_ACOES,
    nb_actions_per_visit AS NUM_ACOES_POR_VISITA,
    date AS DAT_DATA_VISITA,
    SUBSTRING(url FROM LENGTH('pageUrl==') + 1) AS DSC_URL_PAGINA,
    SPLIT_PART(SUBSTRING(url FROM LENGTH('pageUrl==https://brasilparticipativo.presidencia.gov.br/') + 1), '/', 2) AS DSC_NOME_SECAO,
    SPLIT_PART(SUBSTRING(url FROM LENGTH('pageUrl==https://brasilparticipativo.presidencia.gov.br/') + 1), '/', 4) AS DSC_ID_PROCESSO
FROM 
    {{ source('raw', 'devices_detection_get_type') }}
where period = 'week'
    ;
