{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['component_id'],}
    ]
)}}


select 
    component_id,
    "period",
    nb_uniq_visitors, 
    "date"
from {{ ref("matomo_visits_daily") }}
where "period" = 'day'