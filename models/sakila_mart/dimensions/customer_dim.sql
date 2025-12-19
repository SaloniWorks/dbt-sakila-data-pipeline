{{config(
    materialized= 'table',
    schema= 'mart'
)}}

WITH customer_valid AS(
    SELECT c.customer_id,
        c.first_name AS customer_first_name,
        c.last_name AS customer_last_name,
        c.email AS customer_email,
        ci.city AS customer_city,
        co.country AS customer_country,
        greatest(c.dbt_valid_from,a.dbt_valid_from,ci.dbt_valid_from,co.dbt_valid_from) AS valid_from,
        least(coalesce(c.dbt_valid_to,   '9999-12-31'::timestamp),
            coalesce(a.dbt_valid_to,   '9999-12-31'::timestamp),
            coalesce(ci.dbt_valid_to,  '9999-12-31'::timestamp),
            coalesce(co.dbt_valid_to,  '9999-12-31'::timestamp)) AS valid_to_internal
    FROM {{ref('customer_snap')}} c
    LEFT JOIN {{ref('address_snap')}} a
    USING(address_id)
    INNER JOIN {{ ref('city_snap')}} ci 
    ON a.city_id = ci.city_id
    INNER JOIN {{ ref('country_snap')}} co 
    ON ci.country_id = co.country_id
)

SELECT {{dbt_utils.generate_surrogate_key(['customer_id','valid_from'])}} AS customer_key,
    customer_id,
    customer_first_name,
    customer_last_name,
    customer_email,
    customer_city,
    customer_country,
    valid_from,
    CASE WHEN valid_to_internal = '9999-12-31'::timestamp THEN NULL
        ELSE valid_to_internal
    END AS valid_to
FROM customer_valid