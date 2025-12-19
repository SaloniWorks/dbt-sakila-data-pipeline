{{config(
    materialized= 'view',
    schema= 'staging'
)}}

SELECT film_id,
    language_id,
    rental_rate,
    title AS film_title,
    rating AS film_rating,
    rental_duration AS film_duration
FROM {{source('sakila_source_data','film')}}