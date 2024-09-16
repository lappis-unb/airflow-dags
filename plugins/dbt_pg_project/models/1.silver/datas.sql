{{ config(
    materialized='view'
) }}

WITH feriados AS (
    SELECT DISTINCT ON (to_char(date, 'YYYYMMDD')) 
           name,
           to_char(date, 'YYYYMMDD') AS formatted_date
    FROM {{ source('bronze', 'feriados') }}
    ORDER BY to_char(date, 'YYYYMMDD'), name
),
datas AS (
    SELECT   ano_mes_dia
            ,ano_mes
            ,ano
            ,descricao_dia_semana
            ,descricao_mes
            ,numero_mes
            ,numero_dia_mes
            ,numero_dia_semana
            ,numero_dia_ano
            ,numero_semana_mes
            ,final_de_semana
            ,numero_semana_ano

    FROM {{ source('bronze', 'datas') }}
)

SELECT 

             d.ano_mes_dia
            ,d.ano_mes
            ,d.ano
            ,d.descricao_dia_semana
            ,d.descricao_mes
            ,d.numero_mes
            ,d.numero_dia_mes
            ,d.numero_dia_semana
            ,d.numero_dia_ano
            ,d.numero_semana_mes
            ,d.final_de_semana
            ,d.numero_semana_ano
            ,CASE 
                WHEN f.name IS NOT NULL THEN 1 
                ELSE 0 
            END AS flag_feriado
            ,f.name as feriado_nacional
FROM datas d
LEFT JOIN feriados f 
ON d.ano_mes_dia = f.formatted_date
ORDER BY d.ano_mes_dia
