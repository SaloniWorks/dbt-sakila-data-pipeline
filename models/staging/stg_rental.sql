{{config(
    materialized= 'view',
    schema= 'staging'
)}}

SELECT rental_id,
    customer_id,
    inventory_id,
    TO_DATE(rental_date) AS rental_date,
    TO_DATE(return_date) AS return_date
FROM {{source('sakila_source_data','rental')}}