{{ config(
    materialized= 'view',
    schema= 'staging'
)}}

SELECT city_id,
    city,
    country_id,
    last_update
FROM {{source('sakila_source_data','city')}}