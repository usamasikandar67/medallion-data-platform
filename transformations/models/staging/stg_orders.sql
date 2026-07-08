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
    _bronze_batch_id
from {{ source('bronze', 'raw_orders') }}
