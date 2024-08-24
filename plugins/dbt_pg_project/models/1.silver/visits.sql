{{ config(
   meta={
	"datasets_trigger": [
		"bronze_matomo_detailed_visits"
	]
   }
) }}

WITH

base as
(
    SELECT
        *,
        CASE
            WHEN url ~* '\/processes\/([^\/]+)\/?' THEN
                regexp_replace(url, '^.*\/processes\/([^\/]+)\/?.*$', '\1')
            ELSE
                NULL
        END as participatory_process_slug,
        CASE
            WHEN url ~* '\/proposals\/([^\/]+)\/?' THEN
                regexp_replace(url, '^.*\/proposals\/([^\/]+)\/?.*$', '\1')
            ELSE
                NULL
        END as proposal_id
    FROM
        {{ source('bronze', 'matomo_detailed_visits') }}
)

SELECT
    "pageIdAction" as visit_id,
    "idVisit"::varchar as session_id,
    "visitorId"::varchar as visitor_id,
    "serverDate"::date as session_date,
    to_timestamp("serverTimestamp") as session_timestamp,
    to_timestamp(timestamp) as visit_timestamp,
    "timeSpent"::int as visit_duration_seconds,
    "userId"::varchar as user_id,
    CASE
        WHEN proposal_id IS NOT NULL THEN
            'proposal'
        WHEN participatory_process_slug IS NOT NULL THEN
            'participatory_process'
        ELSE
            'bp'
    END as visited_component,
    "visitorType" as visitor_type,
    "referrerType" as referrer_type,
    "referrerTypeName" as referrer_type_name,
    "referrerName" as referrer_name,
    "referrerUrl" as referrer_url,
    "referrerSearchEngineUrl" as referrer_search_engine_url,
    "referrerSocialNetworkUrl" as referrer_social_network_url,
    "deviceType" as device_type,
    "deviceBrand" as device_brand,
    "deviceModel" as device_model,
    "operatingSystemName" as device_os,
    "continent",
    "country",
    upper("countryCode") as country_code,
    "regionCode" as region_code,
    "city",
    url,
    participatory_process_slug,
    process_id as participatory_process_id,
    proposal_id
FROM
    base as b
        LEFT JOIN
    {{ ref('participatory_processes') }} as pp
            ON b.participatory_process_slug = pp.process_slug
