SELECT 
    u.user_id,
    u.username,
    COUNT(p.post_id) AS post_count
FROM 
    {{ ref("users") }} u
LEFT JOIN 
    {{ ref("posts") }} p ON u.user_id = p.user_id
GROUP BY 
    u.user_id, u.username
ORDER BY 
    post_count DESC