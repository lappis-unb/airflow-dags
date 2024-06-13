{{
    config(
        materialized="table",
        meta={
            "datasets_trigger": "postgres://conn_postgres/raw.updated_proposals"
        }
    )
}}

WITH latest_updates AS (
    SELECT
        proposal_id,
        category_name,
        proposal_created_at,
        vote_count,
        total_comments_count,
        state,
        ROW_NUMBER() OVER (PARTITION BY proposal_id ORDER BY proposal_updated_at DESC) AS rn
    FROM
        {{ source('raw', 'updated_proposals') }}
),
proposals_per_state AS (
    SELECT
        COALESCE(state, 'avaliation') AS estado,
        COUNT(DISTINCT proposal_id) AS total_propostas
    FROM
        latest_updates
    WHERE
        rn = 1
    GROUP BY
        COALESCE(state, 'avaliation')
),
total_proposals AS (
    SELECT
        SUM(total_propostas) AS total_geral
    FROM
        proposals_per_state
)
SELECT
    p.estado,
    p.total_propostas,
    ROUND((p.total_propostas / t.total_geral), 2) AS porcentagem
FROM
    proposals_per_state p,
    total_proposals t
