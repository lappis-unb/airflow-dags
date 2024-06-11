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
       proposal_created_at,
       COALESCE(vote_count, 0) AS vote_count,
       COALESCE(total_comments_count, 0) AS total_comments_count,
       ROW_NUMBER() OVER (PARTITION BY proposal_id ORDER BY proposal_updated_at DESC) AS rn
   FROM
       raw.updated_proposals
   WHERE
       proposal_updated_at BETWEEN '{{ var("start_date") }}' AND '{{ var("end_date") }}'
)
SELECT
   COUNT(DISTINCT proposal_id) AS proposals_created,
   SUM(vote_count) AS total_votes,
   SUM(total_comments_count) AS total_comments,
   '{{ var("start_date") }}' AS start_date,
   '{{ var("end_date") }}' AS end_date
FROM
   latest_updates
WHERE
   rn = 1
   AND proposal_created_at BETWEEN '{{ var("start_date") }}' AND '{{ var("end_date") }}'
