{{ config(materialized='view') }}
select
    _row_sequence_id,
    entity_type,
    _change_type,
    _commit_timestamp,
    customer_id,
    first_name,
    last_name,
    email,
    state,
    created_at,
    updated_at,
    _bronze_ingested_at,
    _bronze_batch_id
from {{ source('bronze', 'raw_customers') }}
