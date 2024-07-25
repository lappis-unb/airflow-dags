{{
   config(
       materialized="table",
       meta={
           "datasets_trigger": ["updated_proposals"]
       }
   )
}}


WITH recent_processes AS (
   SELECT
       main_title,
       proposal_updated_at AS latest_updates,
       ROW_NUMBER() OVER (PARTITION BY main_title ORDER BY proposal_updated_at DESC) AS rn
   FROM
       {{ source('raw', 'updated_proposals') }}
),
total_proposals_per_processes AS (
   SELECT
       main_title,
       COUNT(proposal_id) AS total_proposals
   FROM
       {{ source('raw', 'updated_proposals') }}
   GROUP BY
       main_title
),
total_comments_per_processes AS (
   SELECT
       main_title,
       SUM(total_comments_count) AS total_comments
   FROM
       {{ source('raw', 'updated_proposals') }}
   GROUP BY main_title
)
SELECT
   rp.main_title,
   rp.latest_updates,
   tppp.total_proposals,
   tcpp.total_comments
FROM
   recent_processes rp
JOIN
   total_proposals_per_processes tppp ON rp.main_title = tppp.main_title
JOIN
   total_comments_per_processes tcpp ON rp.main_title = tcpp.main_title





