{{ config(
    materialized= 'view',
    schema= 'staging'
)}}

SELECT customer_id,
    first_name,
    last_name,
    email,
    store_id,
    address_id
FROM {{source('sakila_source_data','customer')}}