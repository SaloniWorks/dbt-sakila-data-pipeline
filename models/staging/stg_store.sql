{{config(
    materialized= 'view',
    schema= 'staging'
)}}

SELECT store_id, 
    address_id
FROM {{source('sakila_source_data','store')}}