{{
    config(
        materialized="table",
        meta={
            "datasets_trigger": ["Actions_getPageTitles_weekly"]
        }
    )
}}

WITH total_visits AS (
    SELECT 
        label AS processo,
        SUM(nb_visits) AS visitas_totais,
        -- SUM(nb_uniq_visitors) AS visitantes_unicos,
        SUM(nb_hits) AS numero_total_engajamento,
        SUM(sum_time_spent) AS tempo_total_gasto,
        SUM(nb_hits_with_time_network) AS numero_acessos_tempo_rede,
        SUM(max_time_network) AS tempo_maximo_rede,
        SUM(nb_hits_with_time_server) AS numero_acessos_tempo_servidor,
        SUM(max_time_server) AS tempo_maximo_servidor,
        SUM(nb_hits_with_time_transfer) AS numero_acessos_tempo_transferencia,
        SUM(max_time_transfer) AS tempo_maximo_transferencia,
        SUM(nb_hits_with_time_on_load) AS numero_acessos_tempo_carregamento,
        SUM(max_time_on_load) AS tempo_maximo_carregamento
    FROM
        {{ source('raw', 'Actions_getPageTitles_weekly') }}
    GROUP BY label
)
SELECT
    tv.processo,
    tv.visitas_totais,
    -- tv.visitantes_unicos,
    tv.numero_total_engajamento,
    tv.tempo_total_gasto,
    tv.numero_acessos_tempo_rede,
    tv.tempo_maximo_rede,
    tv.numero_acessos_tempo_servidor,
    tv.tempo_maximo_servidor,
    tv.numero_acessos_tempo_transferencia,
    tv.tempo_maximo_transferencia,
    tv.numero_acessos_tempo_carregamento,
    tv.tempo_maximo_carregamento
FROM
    total_visits tv

-- coluna nb_uniq_visitors não existe em weekly de homolog