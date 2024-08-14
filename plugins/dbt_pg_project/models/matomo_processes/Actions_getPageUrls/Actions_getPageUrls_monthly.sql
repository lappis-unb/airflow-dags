{{
    config(
        materialized = "table",
        meta={
            "datasets_trigger":["Actions_getPageUrls_monthly"]
        }
    )
}}

WITH actions AS (
    SELECT
        REPLACE(label, '/', '') AS secao,
        SUM(nb_visits) AS visitas_totais,
        --SUM(nb_uniq_visitors) AS visitantes_unicos,
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
        {{source('raw','Actions_getPageUrls_monthly')}}
    GROUP BY
        label
)
SELECT
    act.secao,
    act.visitas_totais,
    --act.visitantes_unicos,
    act.numero_total_engajamento,
    act.tempo_total_gasto,
    act.numero_acessos_tempo_rede,
    act.tempo_maximo_rede,
    act.numero_acessos_tempo_servidor,
    act.tempo_maximo_servidor,
    act.numero_acessos_tempo_transferencia,
    act.tempo_maximo_transferencia,
    act.numero_acessos_tempo_carregamento,
    act.tempo_maximo_carregamento
FROM
    actions act