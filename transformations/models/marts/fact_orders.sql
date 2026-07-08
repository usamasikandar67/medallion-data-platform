{{ config(materialized='table') }}
select
    o.order_id,
    coalesce(c.customer_sk, '-1') as customer_sk,
    cast(strftime('%Y%m%d', o.created_at) as integer) as date_key,
    o.order_amount,
    o.order_status,
    o.created_at
from {{ ref('silver_orders') }} o
left join {{ ref('dim_customers') }} c on
    o.customer_id = c.customer_id
    and o.created_at >= c.valid_from
    and o.created_at < c.valid_to
