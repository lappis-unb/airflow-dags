{{ config(
   meta={
	"datasets_trigger": [
		"bronze_decidim_comments_comments"
	]
   }
) }}

WITH

comments_base as
(
    SELECT
        *,
        row_number() over(partition by id order by updated_at DESC) as row_number
    FROM
        {{ source('bronze', 'decidim_comments_comments') }}
    WHERE
        decidim_participatory_space_type = 'Decidim::ParticipatoryProcess'
)

select
	id as comment_id,
	decidim_author_id as user_id,
	lower((regexp_split_to_array(decidim_commentable_type, '::'))[array_length(regexp_split_to_array(decidim_commentable_type, '::'), 1)]) as component_type,
	decidim_commentable_id as commented_component_id,
	lower((regexp_split_to_array(decidim_root_commentable_type, '::'))[array_length(regexp_split_to_array(decidim_root_commentable_type, '::'), 1)]) as root_component_type,
	decidim_root_commentable_id as commented_root_component_id,
	body::json->>'pt-BR' as comment_text,
	created_at,
	updated_at,
	deleted_at
from
	comments_base
WHERE
    row_number = 1