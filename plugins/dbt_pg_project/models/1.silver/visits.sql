{{ config(
   materialized='table',
   meta={
	"datasets_trigger": [
		"bronze_matomo_detailed_visits"
	]
   }
) }}

SELECT    pp.process_id,
          a.*
FROM      (
                    SELECT
                              CASE
                                        WHEN actions.url ~* '\/processes\/([^\/]+)\/?' THEN Regexp_replace(actions.url, '^.*\/processes\/([^\/]+)\/?.*$', '\1')
                                        ELSE NULL
                              END AS participatory_process_slug,
                              CASE
                                        WHEN url ~* '\/proposals\/([^\/]+)\/?' THEN Regexp_replace(actions.url, '^.*\/proposals\/([^\/]+)\/?.*$', '\1')
                                        ELSE NULL
                              END                            AS proposal_id,
                              mdv."idVisit"                  AS session_id,
                              mdv."visitIp"                  AS visit_ip,
                              mdv."visitorId"                AS visitor_id,
                              mdv."goalConversions"          AS goal_conversions,
                              mdv."serverDate"               AS session_date,
                              mdv."visitServerHour"          AS visit_server_hour,
                              mdv."lastActionTimestamp"      AS last_action_timestamp,
                              mdv."lastActionDateTime"       AS last_action_date_time,
                              mdv."serverTimestamp"          AS server_timestamp,
                              mdv."firstActionTimestamp"     AS first_action_timestamp,
                              mdv."userId"                   AS user_id,
                              mdv."visitorType"              AS visitor_type,
                              mdv."visitConverted"           AS visit_converted,
                              mdv."visitCount"               AS visit_count,
                              mdv."daysSinceFirstVisit"      AS days_since_first_visit,
                              mdv."secondsSinceFirstVisit"   AS seconds_since_first_visit,
                              mdv."visitDuration"            AS visit_duration_seconds,
                              mdv.searches                   AS searches,
                              mdv.events                     AS events,
                              mdv."actions"                  AS num_actions,
                              mdv.continent                  AS continent,
                              mdv."continentCode"            AS continent_code,
                              mdv.country                    AS country,
                              mdv."countryCode"              AS country_code,
                              mdv.region                     AS region,
                              mdv."regionCode"               AS region_code,
                              mdv.city                       AS city,
                              mdv."location"                 AS location,
                              mdv."visitLocalHour"           AS visit_local_hour,
                              mdv."daysSinceLastVisit"       AS days_since_last_visit,
                              mdv."secondsSinceLastVisit"    AS seconds_since_last_visit,
                              mdv.resolution                 AS resolution,
                              mdv."actions"                  AS num_actions,
                              actions.type                   AS type,
                              actions.url                    AS url,
                              actions."pageTitle"            AS page_title,
                              actions."pageIdAction"         AS visit_id,
                              actions."idpageview"           AS id_page_view,
                              actions."serverTimePretty"     AS server_time_pretty,
                              actions."pageId"               AS page_id,
                              actions.bandwidth              AS bandwidth,
                              actions."timeSpent"            AS time_spent,
                              actions."timeSpentPretty"      AS time_spent_pretty,
                              actions."pageviewPosition"     AS page_view_position,
                              actions.title                  AS title,
                              actions.subtitle               AS subtitle,
                              actions.timestamp              AS visit_timestamp,
                              actions.bandwidth_pretty       AS bandwidth_pretty
                    FROM      {{ source('bronze', 'matomo_detailed_visits') }} mdv
                    LEFT JOIN lateral jsonb_to_recordset(mdv."actionDetails"::jsonb) AS actions( type text, 
                                                                                                url text, 
                                                                                                "pageTitle" text, 
                                                                                                "pageIdAction" int, 
                                                                                                "idpageview" text, 
                                                                                                "serverTimePretty" text, 
                                                                                                "pageId" bigint, 
                                                                                                bandwidth text, 
                                                                                                "timeSpent" int, 
                                                                                                "timeSpentPretty" text, 
                                                                                                "pageviewPosition" int, 
                                                                                                title text, 
                                                                                                subtitle text, 
                                                                                                timestamp bigint, 
                                                                                                bandwidth_pretty text )
                    ON        true)         AS a
LEFT JOIN {{ ref('participatory_processes') }} AS pp
ON        a.participatory_process_slug = pp.process_slug