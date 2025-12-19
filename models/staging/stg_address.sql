{{ config(
    materialized= 'view',
    schema= 'staging'
)}}

SELECT address_id,
    city_id
FROM {{source('sakila_source_data','address')}}