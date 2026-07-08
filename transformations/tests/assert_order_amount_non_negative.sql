-- Singular test asserting that order amount is always non-negative.
-- Any rows returned represent a failure state.
select
    order_id,
    order_amount
from {{ ref('silver_orders') }}
where order_amount < 0
