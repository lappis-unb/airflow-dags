  {{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['component_id'],}
    ],
    meta={
        "datasets_trigger": "components_matomo_day"
    }
)}}

  
  select 
        SUBSTRING(url FROM LENGTH('pageUrl==') + 1) AS url,
	    SPLIT_PART(SUBSTRING(url FROM LENGTH('pageUrl==https://brasilparticipativo.presidencia.gov.br/') + 1), '/', 2) AS texto_participativo,
	    SPLIT_PART(SUBSTRING(url FROM LENGTH('pageUrl==https://brasilparticipativo.presidencia.gov.br/') + 1), '/', 4) AS processo_id,
  		nb_uniq_visitors
		,nb_users
		,nb_visits
		,nb_actions
		,nb_visits_converted
		,bounce_count
		,sum_visit_length
		,max_actions
		,bounce_rate
		,nb_actions_per_visit
		,avg_time_on_site
		,space
		,method
		,date
		,event_day_id
		,available_day_id
		,available_month_id
		,available_year_id
		,writing_day_id
from raw.visits_summary_get 
where period = 'month'
;
  
 