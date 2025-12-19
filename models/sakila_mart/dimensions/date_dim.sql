{{config(
    materialized= 'table',
    schema= 'mart'
)}}

-- US Holidays for 2024 & 2025
WITH us_holidays AS (
    SELECT * FROM VALUES
        -- ====== YEAR 2024 ======
        ('2024-01-01'::date, 'New Year''s Day', 2024),
        ('2024-01-15'::date, 'Martin Luther King Jr. Day', 2024),
        ('2024-02-19'::date, 'Presidents'' Day', 2024),
        ('2024-05-27'::date, 'Memorial Day', 2024),
        ('2024-06-19'::date, 'Juneteenth National Independence Day', 2024),
        ('2024-07-04'::date, 'Independence Day', 2024),
        ('2024-09-02'::date, 'Labor Day', 2024),
        ('2024-10-14'::date, 'Columbus Day', 2024),
        ('2024-11-11'::date, 'Veterans Day', 2024),
        ('2024-11-28'::date, 'Thanksgiving Day', 2024),
        ('2024-12-25'::date, 'Christmas Day', 2024),

        -- ====== YEAR 2025 ======
        ('2025-01-01'::date, 'New Year''s Day', 2025),
        ('2025-01-20'::date, 'Martin Luther King Jr. Day', 2025),
        ('2025-02-17'::date, 'Presidents'' Day', 2025),
        ('2025-05-26'::date, 'Memorial Day', 2025),
        ('2025-06-19'::date, 'Juneteenth National Independence Day', 2025),
        ('2025-07-04'::date, 'Independence Day', 2025),
        ('2025-09-01'::date, 'Labor Day', 2025),
        ('2025-10-13'::date, 'Columbus Day', 2025),
        ('2025-11-11'::date, 'Veterans Day', 2025),
        ('2025-11-27'::date, 'Thanksgiving Day', 2025),
        ('2025-12-25'::date, 'Christmas Day', 2025)
    AS t(holiday_date, holiday_name, year)
)
SELECT DISTINCT r.rental_date AS date,
    DAYOFWEEK(r.rental_date) AS day_of_week,
    EXTRACT(WEEK FROM r.rental_date) AS week,
    EXTRACT(MONTH FROM r.rental_date) AS month,
    EXTRACT(YEAR FROM r.rental_date) AS year,
    CASE WHEN DAYOFWEEK(r.rental_date) NOT IN (0,6) THEN 1
        ELSE 0
    END AS weekday_flag,
    CASE WHEN h.holiday_date IS NOT NULL
        THEN 1
        ELSE 0
    END AS holiday_flag
FROM {{ref('stg_rental')}} r 
LEFT JOIN us_holidays h 
ON r.rental_date = h.holiday_date