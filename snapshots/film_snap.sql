{% snapshot film_snap %}

{{
    config(
        target_schema='snapshots',
        unique_key='film_id',
        strategy='check',
        check_cols = ['film_rating','film_title','language_id','rental_rate']
    )
}}

select *
from {{ ref('stg_film') }}

{% endsnapshot %}