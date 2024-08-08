{{ config(
    materialized='table',
    schema='dbt',
    meta={
        "datasets_trigger": ["visit_frequency_get"]
    }
) }}

SELECT 
    SUBSTRING(url FROM LENGTH('pageUrl==') + 1) AS dsc_url,
    SPLIT_PART(SUBSTRING(url FROM LENGTH('pageUrl==https://brasilparticipativo.presidencia.gov.br/') + 1), '/', 2) AS dsc_texto_participativo,
    SPLIT_PART(SUBSTRING(url FROM LENGTH('pageUrl==https://brasilparticipativo.presidencia.gov.br/') + 1), '/', 4) AS dsc_processo_id,
    "space" AS dsc_espaco,
    "date" AS data,
    nb_uniq_visitors_new AS num_visitantes_unicos_novos,
    nb_users_new AS num_usuarios_novos,
    nb_visits_new AS num_visitas_novas,
    nb_actions_new AS num_acoes_novas,
    nb_visits_converted_new AS num_visitas_convertidas_novas,
    bounce_count_new AS num_rejeicoes_novas,
    sum_visit_length_new AS num_duracao_total_visitas_novas,
    max_actions_new AS num_max_acoes_por_visita_novas,
    bounce_rate_new AS num_taxa_rejeicao_novas,
    nb_actions_per_visit_new AS num_acoes_por_visita_novas,
    avg_time_on_site_new AS num_tempo_medio_no_site_novas,
    nb_uniq_visitors_returning AS num_visitantes_unicos_retornantes,
    nb_users_returning AS num_usuarios_retornantes,
    nb_visits_returning AS num_visitas_retornantes,
    nb_actions_returning AS num_acoes_retornantes,
    nb_visits_converted_returning AS num_visitas_convertidas_retornantes,
    bounce_count_returning AS num_rejeicoes_retornantes,
    sum_visit_length_returning AS num_duracao_total_visitas_retornantes,
    max_actions_returning AS num_max_acoes_por_visita_retornantes,
    bounce_rate_returning AS num_taxa_rejeicao_retornantes,
    nb_actions_per_visit_returning AS num_acoes_por_visita_retornantes,
    avg_time_on_site_returning AS num_tempo_medio_no_site_retornantes
FROM 
    {{ source('raw', 'visit_frequency_get') }}
WHERE "period" = 'week';
