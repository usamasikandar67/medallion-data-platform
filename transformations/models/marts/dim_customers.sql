{{ config(materialized='table') }}
select
    '-1' as customer_sk,
    'unknown' as customer_id,
    'Unknown' as first_name,
    'Customer' as last_name,
    'unknown@example.com' as email,
    'UNKNOWN' as state,
    '1970-01-01 00:00:00.000' as valid_from,
    '9999-12-31 23:59:59.000' as valid_to,
    1 as is_current
union all
select
    customer_sk,
    customer_id,
    first_name,
    last_name,
    email,
    state,
    valid_from,
    valid_to,
    is_current
from {{ ref('silver_customers') }}
where is_deleted = 0
