WITH users_cte AS (
    SELECT
        1 AS user_id,
        'john_doe' AS username,
        'john.doe@example.com' AS email, 
        'hashedpassword123' AS password_hash,
        'John' AS first_name,
        'Doe' AS last_name, 
        '1985-06-15'::DATE AS date_of_birth,
        CURRENT_TIMESTAMP AS created_at, 
        CURRENT_TIMESTAMP AS updated_at
    UNION ALL
    SELECT
        2,
        'jane_smith',
        'jane.smith@example.com',
        'hashedpassword456',
        'Jane',
        'Smith',
        '1990-09-25'::DATE,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    UNION ALL
    SELECT
        3,
        'alice_jones',
        'alice.jones@example.com',
        'hashedpassword789',
        'Alice',
        'Jones', 
        '1978-02-13'::DATE,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
)

select * from users_cte