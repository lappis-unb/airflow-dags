{{ config(
    materialized='view'
) }}

WITH feriados AS (
    SELECT *,
           to_char(date, 'YYYYMMDD') AS formatted_date
    FROM {{ source('bronze', 'feriados') }}
),
datas AS (
    SELECT *
    FROM {{ source('bronze', 'datas') }}
)

SELECT d.*, 
       CASE 
           WHEN f.name IS NOT NULL THEN 1 
           ELSE 0 
       END AS flag_feriado, 
       f.name 
FROM datas d
LEFT JOIN feriados f 
ON d.ano_mes_dia = f.formatted_date
ORDER BY d.ano_mes_dia