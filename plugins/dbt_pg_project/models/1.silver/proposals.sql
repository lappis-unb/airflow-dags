WITH

proposals_base as
(
    SELECT
        *,
        row_number() over(partition by id order by updated_at DESC) as row_number
    FROM
        {{ source('bronze', 'decidim_proposals_proposals') }}
),

deduped_proposals as (select * from proposals_base where row_number = 1),

scopes_base as
(
    SELECT
        *,
        row_number() over(partition by id, decidim_organization_id order by updated_at DESC) as row_number
    FROM
        {{ source('bronze', 'decidim_scopes') }}
    WHERE
        -- Filter only Brasil Participativo
        decidim_organization_id = 1
),

deduped_scopes as (select * from scopes_base where row_number = 1),

components_base as
(
    SELECT
        *,
        row_number() over(partition by id order by updated_at DESC) as row_number
    FROM
        {{ source('bronze', 'decidim_components') }}
    WHERE
        participatory_space_type = 'Decidim::ParticipatoryProcess'
),

deduped_components as (select * from components_base where row_number = 1),

coauthorships_base as
(
    SELECT
        *,
        row_number() over(partition by id order by updated_at DESC) as row_number
    FROM
        {{ source('bronze', 'decidim_coauthorships') }}
    WHERE
        decidim_author_type = 'Decidim::UserBaseEntity'
        AND coauthorable_type = 'Decidim::Proposals::Proposal'
),

deduped_coauthorships as (select * from coauthorships_base where row_number = 1)

select
	p.id as proposal_id,
    c.participatory_space_id as process_id,
    ca.decidim_author_id as user_id,
	p.state as proposal_status,
	p.created_at,
	p.title::json->>'pt-BR' as proposal_title,
	p.body::json->>'pt-BR' as proposal_text,
	s.name::json->>'pt-BR' as proposal_scope
from 
	deduped_proposals p
		left join
	deduped_scopes s
			on p.decidim_scope_id = s.id
        left join
    deduped_components c
            on p.decidim_component_id = c.id
        left join
    deduped_coauthorships ca
            on p.id = ca.coauthorable_id
