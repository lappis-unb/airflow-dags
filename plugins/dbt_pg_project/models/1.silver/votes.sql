{{ config(
   meta={
	"datasets_trigger": [
        "bronze_decidim_proposals_proposal_votes",
        "bronze_decidim_comments_comments",
        "bronze_decidim_comments_comment_votes"
	]
   }
) }}



WITH

proposal_votes_base as
(
    SELECT
        *,
        row_number() over(partition by id order by updated_at DESC) as row_number
    FROM
        {{ source('bronze', 'decidim_proposals_proposal_votes') }}
),

deduped_proposal_votes as (select * from proposal_votes_base where row_number = 1),

comments_base as
(
    SELECT
        *,
        row_number() over(partition by id order by updated_at DESC) as row_number
    FROM
        {{ source('bronze', 'decidim_comments_comments') }}
    WHERE
        decidim_participatory_space_type = 'Decidim::ParticipatoryProcess'
),

deduped_comments as (select * from comments_base where row_number = 1),

comment_votes_base as
(
    SELECT
        *,
        row_number() over(partition by id order by updated_at DESC) as row_number
    FROM
        {{ source('bronze', 'decidim_comments_comment_votes') }}
),

deduped_comment_votes as (select * from comment_votes_base where row_number = 1),

filtered_comment_votes as (
	SELECT
        cv.*,
        row_number() over(partition by cv.id order by cv.updated_at DESC) as row_number
    FROM
        deduped_comment_votes cv
			LEFT JOIN
		deduped_comments c
				ON cv.decidim_comment_id = c.id
	WHERE
		c.id is not null
)

select
	'proposal_' || id as vote_id,
	id as original_vote_id,
	'proposal' as component_type,
	decidim_proposal_id as voted_component_id,
	decidim_author_id as user_id,
	created_at
from
	deduped_proposal_votes
		
	union all

select
	'comment_' || id as vote_id,
	id as original_vote_id,
	'comment' as component_type,
	decidim_comment_id as voted_component_id,
	decidim_author_id as user_id,
	created_at
from
	filtered_comment_votes