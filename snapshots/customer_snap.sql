{% snapshot customer_snap %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols = ['first_name','last_name','email','address_id']
    )
}}

select customer_id,
    first_name,
    last_name,
    email,
    address_id
from {{ ref('stg_customer') }}

{% endsnapshot %}