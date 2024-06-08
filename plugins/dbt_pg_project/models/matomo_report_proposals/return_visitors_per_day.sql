{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['component_id'],}
    ]
)}}

select
    split_part("url", '/', 7) as component_id,
    "period",
    nb_visits_returning,
    "date",
    "space"
from raw.get_visitfrequency
where "period" = 'day'
