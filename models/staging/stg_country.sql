{{config(
    materialized= 'view',
    schema= 'staging'
)}}

SELECT country_id,
    country,
    last_update
FROM {{source('sakila_source_data','country')}}