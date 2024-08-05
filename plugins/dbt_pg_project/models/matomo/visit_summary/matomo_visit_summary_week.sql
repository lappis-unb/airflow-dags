{{ config(
    materialized='table'
) }}

SELECT 
    SUBSTRING(url FROM LENGTH('pageUrl==') + 1) AS DSC_URL_PAGINA,
    SPLIT_PART(SUBSTRING(url FROM LENGTH('pageUrl==https://brasilparticipativo.presidencia.gov.br/') + 1), '/', 2) AS DSC_NOME_SECAO,
    SPLIT_PART(SUBSTRING(url FROM LENGTH('pageUrl==https://brasilparticipativo.presidencia.gov.br/') + 1), '/', 4) AS DSC_ID_PROCESSO,
    nb_uniq_visitors AS NUM_VISITANTES_UNICOS,
    nb_users AS NUM_TOTAL_USUARIOS,
    nb_visits AS NUM_TOTAL_VISITAS,
    nb_actions AS NUM_TOTAL_ACOES,
    nb_visits_converted AS NUM_VISITAS_CONVERTIDAS,
    bounce_count AS NUM_TOTAL_REJEICOES,
    sum_visit_length AS NUM_DURACAO_TOTAL_VISITAS,
    max_actions AS NUM_MAX_ACOES_POR_VISITA,
    bounce_rate AS NUM_TAXA_REJEICAO,
    nb_actions_per_visit AS NUM_ACOES_POR_VISITA,
    avg_time_on_site AS NUM_TEMPO_MEDIO_NO_SITE,
    space AS DSC_ESPACO,
    method AS DSC_METODO,
    date AS DAT_DATA_VISITA,
    event_day_id AS DAT_ID_DIA_EVENTO,
    available_day_id AS DAT_ID_DIA_DISPONIVEL,
    available_month_id AS DAT_ID_MES_DISPONIVEL,
    available_year_id AS DAT_ID_ANO_DISPONIVEL,
    writing_day_id AS DAT_ID_DIA_ESCRITA
FROM
    raw.visits_summary_get
WHERE
    period = 'week'
