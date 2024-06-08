{{
    config(
        materialized="table",
        indexes=[
            {
                "columns": ["component_id", "period", "date", "country"],
            }
        ],
        meta={
            "datasets_trigger": "user_country_get_country_bronze,user_country_get_region_bronze"
        }
    )
}}


select
    split_part(region."url", '/', 7) as component_id,
    region."period",
    region."date",
    region."space",
    region.country,
    region.region,
    region.region_name,
    cast(region.sum_daily_nb_uniq_visitors as float8)
    / cast(contry.sum_daily_nb_uniq_visitors as float8) as region_access_ratio,
    contry.sum_daily_nb_uniq_visitors as contry_sum_daily_nb_uniq_visitors,
    region.sum_daily_nb_uniq_visitors as region_sum_daily_nb_uniq_visitors
from {{ source("matomo", "user_country_get_country") }} as contry
inner join
    {{ source("matomo", "user_country_get_region") }} as region
    on region.url = contry.url
    and region."date" = contry."date"
    and region.event_day_id = contry.event_day_id
    and region."period" = contry."period"
    and region.country = contry.code
where region.country = 'br'
