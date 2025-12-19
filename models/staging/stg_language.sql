{{config(
    materialized= 'view',
    schema= 'staging'
)}}

SELECT language_id,
    name AS film_language,
    last_update
FROM {{source('sakila_source_data','language')}}