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
       proposal_title,
       category_name,
       COALESCE(vote_count, 0) AS vote_count,
       COALESCE(total_comments_count, 0) AS total_comments_count,
       ROW_NUMBER() OVER (PARTITION BY proposal_id ORDER BY proposal_updated_at DESC) AS rn
   FROM
       raw.updated_proposals
   WHERE
       proposal_updated_at BETWEEN '{{ var("start_date") }}' AND '{{ var("end_date") }}'
)
SELECT
   proposal_title,
   category_name,
   vote_count,
   total_comments_count,
   '{{ var("start_date") }}' AS data_inicial,
   '{{ var("end_date") }}' AS data_final
FROM
   latest_updates
WHERE
   rn = 1
ORDER BY
   vote_count DESC
LIMIT 20
