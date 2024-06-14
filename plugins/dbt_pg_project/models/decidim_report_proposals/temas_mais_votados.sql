{{
    config(
        materialized="table",
        meta={
            "datasets_trigger": "updated_proposals"
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
       ROW_NUMBER() OVER (PARTITION BY proposal_id ORDER BY proposal_updated_at DESC) AS rn
   FROM
       {{ source('raw', 'updated_proposals') }}
)
SELECT
   category_name AS tema,
   COUNT(DISTINCT proposal_id) AS total_propostas,
   SUM(vote_count) AS total_votos,
   SUM(total_comments_count) AS total_comentarios
FROM
   latest_updates
WHERE
   rn = 1
GROUP BY
   category_name
