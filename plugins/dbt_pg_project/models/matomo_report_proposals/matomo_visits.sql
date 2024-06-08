{{
    config(
        materialized="table",
        indexes=[
            {
                "columns": ["component_id", "period", "date"],
            }
        ],
    )
}}


select
    split_part(gv."url", '/', 7) as component_id,
    gv."period",
    gv."date",
    gv."space",
    gv.nb_uniq_visitors,
    gv.bounce_rate,
    gf.nb_visits_returning,
    gf.nb_visits_new
from {{ source("matomo", "visits_summary_get") }} gv
inner join
    {{ source("matomo", "visit_frequency_get") }} as gf
    on gv.url = gf.url
    and gv."date" = gf."date"
    and gv.event_day_id = gf.event_day_id
    and gv."period" = gf."period"
