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
        raw.updated_proposals
    WHERE
        proposal_updated_at BETWEEN '{{ var("start_date") }}' AND '{{ var("end_date") }}'
),
proposals_per_state AS (
    SELECT
        COALESCE(state, 'avaliation') AS estado, -- verificar se está correto
        COUNT(DISTINCT proposal_id) AS total_propostas,
        '{{ var("start_date") }}' AS data_inicial,
        '{{ var("end_date") }}' AS data_final
    FROM
        latest_updates
    WHERE
        rn = 1
        AND proposal_created_at BETWEEN '{{ var("start_date") }}' AND '{{ var("end_date") }}'
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
    ROUND((p.total_propostas / t.total_geral), 2) AS porcentagem,
    data_inicial,
    data_final
FROM
    proposals_per_state p,
    total_proposals t
