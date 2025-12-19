{% snapshot city_snap %}

{{
    config(
        target_schema='snapshots',
        unique_key='city_id',
        strategy='timestamp',
        updated_at= 'last_update'
    )
}}

select *
from {{ ref('stg_city') }}

{% endsnapshot %}