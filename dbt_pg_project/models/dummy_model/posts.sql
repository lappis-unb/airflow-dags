WITH posts_cte AS (
    SELECT
        1 AS post_id,
        1 AS user_id,
        'Post 1 Title' AS title,
        'Content of post 1' AS content, 
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
    UNION ALL
    SELECT
        2,
        1,
        'Post 2 Title',
        'Content of post 2',
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    UNION ALL
    SELECT
        3,
        2,
        'Post 3 Title',
        'Content of post 3',
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
)

select * from posts_cte