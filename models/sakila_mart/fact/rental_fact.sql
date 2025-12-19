{{ config(
    materialized='incremental',
    schema='mart',
    incremental_strategy='merge',
    on_schema_change='sync',
    unique_key='rental_key'
) }}

WITH incremental_cutoff as (
    {% if is_incremental() %}
      SELECT dateadd('month', -1, max(date)) as cutoff_month
      FROM {{ this }}
    {% else %}
      SELECT to_date('1900-01-01') as cutoff_month
    {% endif %}
),
metric_cal AS (
    SELECT r.rental_id,
        f.film_id,
        i.store_id,
        r.customer_id,
        r.rental_date,
        CEIL(DATEDIFF('day',r.rental_date,r.return_date)) AS rental_duration,
        CASE WHEN return_date IS NULL 
            THEN NULL
            WHEN CEIL(DATEDIFF('day',r.rental_date,r.return_date)) >= f.film_duration
            THEN (CEIL(DATEDIFF('day',r.rental_date,r.return_date)) - f.film_duration)* 1
        END AS late_fee,
        f.rental_rate AS rental_price
    FROM {{ref('stg_rental')}} r
    LEFT JOIN {{ref('stg_inventory')}} i
    USING(inventory_id)
    INNER JOIN {{ ref('stg_film')}} f
    USING(film_id)
    where date_trunc('month', r.rental_date)::date >= (select cutoff_month from incremental_cutoff)
),

Total_Amt AS (
    SELECT p.rental_id, SUM(p.amount_paid) AS amount_paid
    FROM {{ref('stg_payment')}} p
    WHERE EXISTS (
    select 1
    from metric_cal mc
    where mc.rental_id = p.rental_id
  )
    GROUP BY p.rental_id
    
)

SELECT {{dbt_utils.generate_surrogate_key(['mc.rental_id'])}} AS rental_key,
    f.film_key,
    c.customer_key,
    s.store_key,
    d.date,
    mc.rental_id,
    mc.rental_duration,
    mc.rental_price,
    mc.late_fee,
    tm.amount_paid
FROM metric_cal mc
LEFT JOIN {{ref('customer_dim')}} c
USING(CUSTOMER_ID)
LEFT JOIN {{ref('film_dim')}} f
ON mc.film_id = f.film_id
LEFT JOIN {{ref('store_dim')}} s
ON mc.store_id = s.store_id
LEFT JOIN {{ ref('date_dim')}} d 
ON mc.rental_date = d.date
LEFT JOIN Total_Amt tm
ON mc.rental_id = tm.rental_id