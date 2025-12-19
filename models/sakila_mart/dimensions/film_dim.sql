{{config(
    materialized= 'table',
    schema= 'mart'
)}}

WITH film_lang_valid AS (
    SELECT 
        f.film_id,
        f.rental_rate,
        f.film_title,
        f.film_rating,
        l.film_language,
        greatest(f.dbt_valid_from,l.dbt_valid_from) AS valid_from,
        least(coalesce(f.dbt_valid_to,   '9999-12-31'::timestamp),
            coalesce(l.dbt_valid_to,   '9999-12-31'::timestamp)) AS valid_to_internal
    FROM {{ref('film_snap')}} f
    LEFT JOIN {{ ref('language_snap')}} l
    USING(language_id)
)

SELECT {{ dbt_utils.generate_surrogate_key(['flv.film_id','flv.valid_from'])}} AS film_key,
    flv.film_id,
    flv.rental_rate,
    flv.film_title,
    flv.film_rating,
    flv.film_language,
    flv.valid_from,
    CASE WHEN flv.valid_to_internal = '9999-12-31'::timestamp THEN NULL
        ELSE flv.valid_to_internal
    END AS valid_to
FROM film_lang_valid flv