{{ config(
    meta={
	"datasets_trigger": [
		"bronze_dates"
	]
   }
) }}

WITH holidays AS (
    SELECT DISTINCT ON (to_char(date, 'YYYYMMDD')) 
           name,
           to_char(date, 'YYYYMMDD') AS formatted_date
    FROM {{ source('bronze', 'holidays') }}
    ORDER BY to_char(date, 'YYYYMMDD'), name
),
dates AS (
    SELECT   year_month_day
            ,year_month
            ,year
            ,week_day_description
            ,month_description
            ,month_number
            ,day_of_month
            ,day_of_week
            ,day_of_year
            ,week_of_month
            ,weekend
            ,week_of_year

    FROM {{ source('bronze', 'dates') }}
)

SELECT 
             d.year_month_day
            ,d.year_month
            ,d.year
            ,d.week_day_description
            ,d.month_description
            ,d.month_number
            ,d.day_of_month
            ,d.day_of_week
            ,d.day_of_year
            ,d.week_of_month
            ,d.weekend
            ,d.week_of_year
            ,CASE 
                WHEN f.name IS NOT NULL THEN 1 
                ELSE 0 
            END AS holiday_flag
            ,f.name as national_holiday
FROM dates d
LEFT JOIN holidays f 
ON d.year_month_day = f.formatted_date
ORDER BY d.year_month_day
