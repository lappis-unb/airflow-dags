{{ config(
   meta={
	"datasets_trigger": [
		"bronze_decidim_users"
	]
   }
) }}


WITH

users_base as
(
    SELECT
        *,
        row_number() over(partition by id order by updated_at DESC) as row_number
    FROM
        {{ source('bronze', 'decidim_users') }}
)

SELECT
    id as user_id,
    nickname as username,
    email as user_email,
    created_at,
    updated_at,
    last_sign_in_at as last_activity_at,
    roles::json as user_roles
FROM
    users_base
WHERE
    row_number = 1