

{{ config(
    materialized='table',
    full_refresh=True,
    indexes=[
      {'columns': ['id_proposta', 'titulo_proposta']}
      ]
) }}

WITH proposals_dates AS (
    SELECT
        p.proposal_title,
        p.proposal_status,
        p.process_id,
        pp.process_title,
        p.proposal_id,
        p.proposal_category,
        generate_series(
            p.created_at::date,  -- Data de criação da proposta
            CURRENT_DATE,        -- Até a data atual
            '1 day'::interval
        )::date AS data_do_voto
    FROM
        {{ ref('proposals') }} p
    JOIN {{ ref('participatory_processes') }} pp
        ON p.process_id = pp.process_id
),
bounce_rates AS (
        SELECT 
            session_date::date as data_rejeicao,
            proposal_id::INTEGER,
            COUNT(DISTINCT session_id) AS qtd_visitas,
            SUM(CASE WHEN num_actions = 1 THEN 1 ELSE 0 END) AS qtd_rejeicoes,
            SUM(CASE WHEN num_actions = 1 THEN 1 ELSE 0 END) / COUNT(DISTINCT session_id)  as bounce_rate
        FROM 
            dev_silver.visits v
        WHERE 
            proposal_id IS NOT null
            AND proposal_id ~ '^\d+$'
        GROUP BY 
            session_date, proposal_id
),
daily_votes AS (
    SELECT
        v.created_at::date AS data_do_voto,
        v.voted_component_id AS proposal_id,
        COUNT(v.voted_component_id) AS qtd_votos
    FROM
        {{ ref('votes') }} v
    GROUP BY
        v.created_at::date,
        v.voted_component_id
),
daily_comments AS (
    SELECT
        commented_root_component_id AS proposal_id,
        created_at::date AS data_comentario,
        COUNT(0) AS qtd_comentarios
    FROM
        {{ ref('comments') }} c
    GROUP BY
        commented_root_component_id,
        created_at::date
)
SELECT
    proposals_dates.data_do_voto AS data_operacao,
    proposals_dates.process_id AS id_processo,
    proposals_dates.proposal_id AS id_proposta,
    proposals_dates.proposal_title AS titulo_proposta,
    proposals_dates.proposal_status AS status_proposta,
    proposals_dates.process_title AS titulo_processo,
    proposals_dates.proposal_category AS eixo_tematico,
    COALESCE(bounce_rates.qtd_visitas, 0) AS qtd_visitas,
    COALESCE(bounce_rates.qtd_rejeicoes, 0) AS qtd_rejeicao,
    COALESCE(daily_votes.qtd_votos, 0) AS qtd_votos,
    COALESCE(daily_comments.qtd_comentarios, 0) AS qtd_comentarios
FROM
    proposals_dates
LEFT JOIN bounce_rates
    ON proposals_dates.proposal_id = bounce_rates.proposal_id
    AND proposals_dates.data_do_voto = bounce_rates.data_rejeicao
LEFT JOIN daily_votes
    ON proposals_dates.proposal_id = daily_votes.proposal_id
    AND proposals_dates.data_do_voto = daily_votes.data_do_voto
LEFT JOIN daily_comments
    ON proposals_dates.proposal_id = daily_comments.proposal_id
    AND proposals_dates.data_do_voto = daily_comments.data_comentario
