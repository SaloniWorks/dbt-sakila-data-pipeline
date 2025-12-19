{% snapshot language_snap %}

{{
    config(
        target_schema='snapshots',
        unique_key='language_id',
        strategy='timestamp',
        updated_at= 'last_update'
    )
}}

select *
from {{ ref('stg_language') }}

{% endsnapshot %}