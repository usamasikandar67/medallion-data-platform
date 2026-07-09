{{ config(materialized='table') }}
with cust_history as (
    select
        _row_sequence_id,
        customer_id,
        first_name,
        last_name,
        email,
        upper(trim(state)) as state,
        _commit_timestamp as valid_from,
        lead(_commit_timestamp, 1, '9999-12-31 23:59:59') over (
            partition by customer_id 
            order by _commit_timestamp asc, _row_sequence_id asc
        ) as valid_to,
        _change_type,
        _bronze_batch_id
    from {{ ref('stg_customers') }}
    where _change_type != 'UPDATE_BEFORE'
)
select
    customer_id || '_' || _row_sequence_id as customer_sk,
    customer_id,
    first_name,
    substr(last_name, 1, 1) || '.' as last_name,
    case when email like '%@%' then substr(email, 1, 2) || '***@' || substr(email, instr(email, '@') + 1) else '***' end as email,
    state,
    valid_from,
    valid_to,
    case when valid_to = '9999-12-31 23:59:59' and _change_type != 'DELETE' then 1 else 0 end as is_current,
    case when _change_type = 'DELETE' then 1 else 0 end as is_deleted,
    _bronze_batch_id
from cust_history
