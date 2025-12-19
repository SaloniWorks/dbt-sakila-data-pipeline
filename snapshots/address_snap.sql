{% snapshot address_snap %}

{{
    config(
        target_schema='snapshots',
        unique_key='address_id',
        strategy='check',
        check_cols = ['city_id']
    )
}}

select *
from {{ ref('stg_address') }}

{% endsnapshot %}