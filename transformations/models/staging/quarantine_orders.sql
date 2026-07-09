{{ config(materialized='view') }}
select
    _row_sequence_id,
    entity_type,
    _change_type,
    _commit_timestamp,
    order_id,
    customer_id,
    order_amount,
    order_status,
    created_at,
    updated_at,
    _bronze_ingested_at,
    _bronze_batch_id,
    'Negative order amount' as quarantine_reason
from {{ source('bronze', 'raw_orders') }}
where order_amount < 0
