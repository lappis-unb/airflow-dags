{{ config(
    materialized='table',
) }}

WITH latest_updates AS (
   SELECT
       proposal_id,
       category_name,
       proposal_created_at,
       vote_count,
       total_comments_count,
       ROW_NUMBER() OVER (PARTITION BY proposal_id ORDER BY proposal_updated_at DESC) AS rn
   FROM
       raw.updated_proposals
   WHERE
       proposal_updated_at BETWEEN '{{ var("start_date") }}' AND '{{ var("end_date") }}'
)
SELECT
   category_name AS tema,
   COUNT(DISTINCT proposal_id) AS total_propostas,
   SUM(vote_count) AS total_votos,
   SUM(total_comments_count) AS total_comentarios,
   '{{ var("start_date") }}' AS data_inicial, 
   '{{ var("end_date") }}' AS data_final
FROM
   latest_updates
WHERE
   rn = 1
   AND proposal_created_at BETWEEN '{{ var("start_date") }}' AND '{{ var("end_date") }}'
GROUP BY
   category_name
