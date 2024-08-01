{{
   config(
       materialized="table",
       meta={
           "datasets_trigger": ["updated_proposals"]
       }
   )
}}

WITH unique_proposals AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY proposal_id ORDER BY proposal_updated_at DESC) AS rn
    FROM
        {{ source('raw', 'updated_proposals') }}
)
SELECT
   main_title,participatory_space_id, component_id,proposal_id,proposal_created_at, proposal_published_at, proposal_updated_at

FROM
    unique_proposals
WHERE
    rn = 1
