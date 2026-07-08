{{ config(materialized='table') }}
with ordered_events as (
    select
        order_id,
        customer_id,
        cast(order_amount as real) as order_amount,
        order_status,
        created_at,
        updated_at,
        _change_type,
        _bronze_batch_id,
        row_number() over (
            partition by order_id 
            order by _commit_timestamp desc, _row_sequence_id desc
        ) as rn
    from {{ ref('stg_orders') }}
)
select
    order_id,
    customer_id,
    order_amount,
    order_status,
    created_at,
    updated_at,
    _bronze_batch_id
from ordered_events
where rn = 1 and _change_type != 'DELETE'
