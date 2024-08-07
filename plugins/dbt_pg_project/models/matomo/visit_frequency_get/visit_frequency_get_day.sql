{{ config(
    materialized='table',
    schema='dbt'
) }}

  SELECT 
  	SUBSTRING(url FROM LENGTH('pageUrl==') + 1) AS DSC_URL,
    SPLIT_PART(SUBSTRING(url FROM LENGTH('pageUrl==https://brasilparticipativo.presidencia.gov.br/') + 1), '/', 2) AS DSC_texto_participativo,
    SPLIT_PART(SUBSTRING(url FROM LENGTH('pageUrl==https://brasilparticipativo.presidencia.gov.br/') + 1), '/', 4) AS DSC_processo_id,
    nb_uniq_visitors_new AS NUM_VISITANTES_UNICOS_NOVOS,
    nb_users_new AS NUM_USUARIOS_NOVOS,
    nb_visits_new AS NUM_VISITAS_NOVAS,
    nb_actions_new AS NUM_ACOES_NOVAS,
    nb_visits_converted_new AS NUM_VISITAS_CONVERTIDAS_NOVAS,
    bounce_count_new AS NUM_REJEICOES_NOVAS,
    sum_visit_length_new AS NUM_DURACAO_TOTAL_VISITAS_NOVAS,
    max_actions_new AS NUM_MAX_ACOES_POR_VISITA_NOVAS,
    bounce_rate_new AS NUM_TAXA_REJEICAO_NOVAS,
    nb_actions_per_visit_new AS NUM_ACOES_POR_VISITA_NOVAS,
    avg_time_on_site_new AS NUM_TEMPO_MEDIO_NO_SITE_NOVAS,
    nb_uniq_visitors_returning AS NUM_VISITANTES_UNICOS_RETORNANTES,
    nb_users_returning AS NUM_USUARIOS_RETORNANTES,
    nb_visits_returning AS NUM_VISITAS_RETORNANTES,
    nb_actions_returning AS NUM_ACOES_RETORNANTES,
    nb_visits_converted_returning AS NUM_VISITAS_CONVERTIDAS_RETORNANTES,
    bounce_count_returning AS NUM_REJEICOES_RETORNANTES,
    sum_visit_length_returning AS NUM_DURACAO_TOTAL_VISITAS_RETORNANTES,
    max_actions_returning AS NUM_MAX_ACOES_POR_VISITA_RETORNANTES,
    bounce_rate_returning AS NUM_TAXA_REJEICAO_RETORNANTES,
    nb_actions_per_visit_returning AS NUM_ACOES_POR_VISITA_RETORNANTES,
    avg_time_on_site_returning AS NUM_TEMPO_MEDIO_NO_SITE_RETORNANTES,
    "space" AS DSC_ESPACO_PARTICIPATIVO,
    "method" AS DSC_METODO,
    "date" AS DAT_DATA,
    event_day_id AS DAT_ID_DIA_EVENTO,
    available_day_id AS DAT_ID_DIA_DISPONIVEL,
    available_month_id AS DAT_ID_MES_DISPONIVEL,
    available_year_id AS DAT_ID_ANO_DISPONIVEL,
    writing_day_id AS DAT_ID_DIA_ESCRITA,
    "period" AS DSC_PERIODO
FROM 
    raw.visit_frequency_get 
WHERE period = 'day'
    ;
