{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['component_id'],}
    ],
    meta={
        "datasets_trigger": "visit_frequency_get"
    }
)}}


select
    split_part("url", '/', 7) as component_id,
    "period",
    nb_visits_new,
    "date",
    "space"
from
    raw.get_visitfrequency
where "period" = 'day'