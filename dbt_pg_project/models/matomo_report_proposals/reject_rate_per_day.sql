{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['component_id'],}
    ]
)}}


select bounce_rate,
    split_part("url", '/', 7) as component_id, 
    "period",
    "date",
    "space"
from raw.get_visitssummary
where "period" = 'day'