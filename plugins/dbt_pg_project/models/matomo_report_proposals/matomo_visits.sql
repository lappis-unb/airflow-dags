{{
    config(
        materialized="table",
        indexes=[
            {
                "columns": ["component_id", "period", "date"],
            }
        ],
        meta={
            "datasets_trigger": "visits_summary_get_bronze,visit_frequency_get_bronze"
        }
    )
}}


select
    split_part(gv."url", '/', 7) as component_id,
    gv."period",
    gv."date",
    gv."space",
    gv.nb_uniq_visitors,
    cast(split_part(gv.bounce_rate, '%', 1) as float4) as bounce_rate,
    gf.nb_visits_returning,
    gf.nb_visits_new
from {{ source("matomo", "visits_summary_get") }} gv
inner join
    {{ source("matomo", "visit_frequency_get") }} as gf
    on gv.url = gf.url
    and gv."date" = gf."date"
    and gv.event_day_id = gf.event_day_id
    and gv."period" = gf."period"
