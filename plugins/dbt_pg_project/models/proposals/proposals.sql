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
   *
FROM
    unique_proposals
WHERE
    rn = 1
