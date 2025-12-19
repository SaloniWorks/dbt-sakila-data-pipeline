{{config(
    materialized= 'view',
    schema= 'staging'
)}}

SELECT payment_id,
    rental_id,
    amount AS amount_paid
FROM {{source('sakila_source_data','payment')}}