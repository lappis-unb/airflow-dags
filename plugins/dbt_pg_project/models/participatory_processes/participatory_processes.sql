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
)
SELECT
   rp.main_title,
   rp.latest_updates
FROM
   recent_processes rp





