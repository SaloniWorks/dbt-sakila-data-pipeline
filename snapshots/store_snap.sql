{% snapshot store_snap %}

{{
    config(
        target_schema='snapshots',
        unique_key='store_id',
        strategy='check',
        check_cols = ['address_id']
    )
}}

select *
from {{ ref('stg_store') }}

{% endsnapshot %}