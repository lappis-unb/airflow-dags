{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['component_id'],}
    ]
)}}


select
    split_part(gv."url", '/', 7) as component_id,
    gv."period",
    gv."date",
    gv."space",
    gv.nb_uniq_visitors,
    gv.bounce_rate,
    gf.nb_visits_returning,
    gf.nb_visits_new
from
    raw.get_visitssummary gv
    inner join raw.get_visitfrequency as gf on gv.url = gf.url
    and gv."date" = gf."date"
    and gv.event_day_id = gf.event_day_id
    and gv.available_day_id = gf.available_day_id
    and gv.available_month_id = gf.available_month_id
    and gv.available_year_id = gf.available_year_id
    and gv.writing_day_id = gf.writing_day_id
    and gv."period" = gf."period"
where gv."period" = 'week'