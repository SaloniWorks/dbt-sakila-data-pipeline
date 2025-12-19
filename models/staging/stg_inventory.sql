{{config(
    materialized= 'view',
    schema= 'staging'
)}}

SELECT inventory_id, film_id, store_id, last_update
FROM {{source('sakila_source_data','inventory')}}