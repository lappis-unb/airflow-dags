{{ config(
    materialized='table',
    schema='dbt'
) }}


SELECT 
	SUBSTRING(url FROM LENGTH('pageUrl==') + 1) AS DSC_URL,
    SPLIT_PART(SUBSTRING(url FROM LENGTH('pageUrl==https://brasilparticipativo.presidencia.gov.br/') + 1), '/', 2) AS DSC_texto_participativo,
    SPLIT_PART(SUBSTRING(url FROM LENGTH('pageUrl==https://brasilparticipativo.presidencia.gov.br/') + 1), '/', 4) AS DSC_processo_id,
    "label" AS DSC_DISPOSITIVO,
    nb_uniq_visitors AS NUM_VISITANTES_UNICOS,
    nb_visits AS NUM_TOTAL_VISITAS,
    nb_actions AS NUM_TOTAL_ACOES,
    nb_users AS NUM_TOTAL_USUARIOS,
    max_actions AS NUM_MAX_ACOES_POR_VISITA,
    sum_visit_length AS NUM_DURACAO_TOTAL_VISITAS,
    bounce_count AS NUM_TOTAL_REJEICOES,
    nb_visits_converted AS NUM_VISITAS_CONVERTIDAS,
    segment AS DSC_SEGMENTO,
    logo AS DSC_LOGO,
    "space" AS DSC_ESPACO_PARTICIPATIVO,
    "method" AS DSC_METODO,
    "date" AS DAT_DATA,
    event_day_id AS DAT_ID_DIA_EVENTO,
    available_day_id AS DAT_ID_DIA_DISPONIVEL,
    available_month_id AS DAT_ID_MES_DISPONIVEL,
    available_year_id AS DAT_ID_ANO_DISPONIVEL,
    writing_day_id AS DAT_ID_DIA_ESCRITA,
    "period" AS DSC_PERIODO,
    sum_daily_nb_users AS NUM_TOTAL_USUARIOS_DIARIOS,
    sum_daily_nb_uniq_visitors AS NUM_VISITANTES_UNICOS_DIARIOS
FROM 
    raw.devices_detection_get_type
where period = 'month'
    ;
