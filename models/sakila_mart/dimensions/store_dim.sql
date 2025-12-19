{{config(
    materialized= 'table',
    schema= 'mart'
)}}

WITH Valids AS(
    SELECT s.store_id,
        ci.city AS store_city,
        co.country AS store_country,
        greatest(s.dbt_valid_from,a.dbt_valid_from,ci.dbt_valid_from,co.dbt_valid_from) AS valid_from,
        least(coalesce(s.dbt_valid_to,   '9999-12-31'::timestamp),
            coalesce(a.dbt_valid_to,   '9999-12-31'::timestamp),
            coalesce(ci.dbt_valid_to,  '9999-12-31'::timestamp),
            coalesce(co.dbt_valid_to,  '9999-12-31'::timestamp)) AS valid_to_internal
    FROM {{ref('store_snap')}} s 
    LEFT JOIN {{ ref('address_snap')}} a 
    USING(address_id)
    INNER JOIN {{ ref('city_snap')}} ci 
    ON a.city_id = ci.city_id
    INNER JOIN {{ ref('country_snap')}} co 
    ON ci.country_id = co.country_id   
)

SELECT {{ dbt_utils.generate_surrogate_key(['v.store_id','v.valid_from'])}} AS store_key,
    v.store_id,
    v.store_city,
    v.store_country,
    v.valid_from,
    CASE WHEN v.valid_to_internal = '9999-12-31'::timestamp THEN NULL
        ELSE v.valid_to_internal
    END AS valid_to
FROM Valids v