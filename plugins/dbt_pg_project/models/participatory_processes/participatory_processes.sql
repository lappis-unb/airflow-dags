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
       main_title AS title_process,
       proposal_updated_at AS latest_updates,
       ROW_NUMBER() OVER (PARTITION BY main_title ORDER BY proposal_updated_at DESC) AS rn
   FROM
       {{ source('raw', 'updated_proposals') }}
),
total_proposals_per_processes AS (
   SELECT
       main_title AS title_process,
       COUNT(proposal_id) AS total_proposals
   FROM
       {{ source('raw', 'updated_proposals') }}
   GROUP BY
       title_process
),
total_comments_per_processes AS (
   SELECT
       main_title  AS title_process,
       SUM(total_comments_count) AS total_comments
   FROM
       {{ source('raw', 'updated_proposals') }}
   GROUP BY title_process
),
total_votes_per_processes AS (
   SELECT
       main_title  AS title_process,
       SUM(vote_count) AS total_votes
   FROM
       {{ source('raw','updated_proposals') }}
   GROUP BY
       title_process
)
SELECT
   rp.title_process,
   rp.latest_updates,
   tppp.total_proposals,
   tcpp.total_comments,
   tvpp.total_votes
FROM
   recent_processes rp
JOIN
   total_proposals_per_processes tppp ON rp.title_process = tppp.title_process
JOIN
   total_comments_per_processes tcpp ON rp.title_process = tcpp.title_process
JOIN
   total_votes_per_processes tvpp ON rp.title_process = tvpp.title_process
WHERE
   rp.rn = 1





