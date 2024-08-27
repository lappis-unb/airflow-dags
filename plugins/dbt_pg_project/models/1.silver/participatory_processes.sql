{{ config(
   meta={
	"datasets_trigger": [
		"bronze_decidim_participatory_processes",
		"bronze_decidim_participatory_process_types",
		"bronze_decidim_areas",
		"bronze_decidim_area_types"
	]
   }
) }}


WITH

participatory_processes_base as
(
    SELECT
        *,
        row_number() over(partition by id, decidim_organization_id order by updated_at DESC) as row_number
    FROM
        {{ source('bronze', 'decidim_participatory_processes') }}
),

deduped_participatory_processes as (select * from participatory_processes_base where row_number = 1),

participatory_process_types_base as
(
    SELECT
        *,
        row_number() over(partition by id, decidim_organization_id order by updated_at DESC) as row_number
    FROM
        {{ source('bronze', 'decidim_participatory_process_types') }}
),

deduped_participatory_process_types as (select * from participatory_process_types_base where row_number = 1),

areas_base as
(
    SELECT
        *,
        row_number() over(partition by id, decidim_organization_id order by updated_at DESC) as row_number
    FROM
        {{ source('bronze', 'decidim_areas') }}
),

deduped_areas as (select * from areas_base where row_number = 1),

area_types_base as
(
    SELECT
        *,
        row_number() over(partition by id, decidim_organization_id) as row_number
    FROM
        {{ source('bronze', 'decidim_area_types') }}
),

deduped_area_types as (select * from area_types_base where row_number = 1)

select
	p.id as process_id,
	pt.title::json->>'pt-BR' as process_type,
    p.slug as process_slug,
	p.title::json->>'pt-BR' as process_title,
	p.subtitle::json->>'pt-BR' as process_subtitle,
	p.description::json->>'pt-BR' as description,
	p.short_description::json->>'pt-BR' as short_description,
	a.name::json->>'pt-BR' as responsible_area,
	at.name::json->>'pt-BR' as responsible_type,
	p.created_at,
	p.updated_at,
	p.published_at,
	p.start_date,
	p.end_date,
	p.reference,
	p.follows_count
from
	deduped_participatory_processes p
		left join
	deduped_participatory_process_types pt 
			on p.decidim_participatory_process_type_id = pt.id
            and p.decidim_organization_id = pt.decidim_organization_id
		left join
	deduped_areas a
			on p.decidim_area_id = a.id
            and p.decidim_organization_id = a.decidim_organization_id
		left join
	deduped_area_types at
			on a.area_type_id = at.id
            and a.decidim_organization_id = at.decidim_organization_id
where
    -- Filter only Brasil Participativo
    p.decidim_organization_id = 1