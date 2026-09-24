-- A cancelled or returned order still EXISTS and keeps its quantity, but it
-- must bring in zero revenue. Excluding it entirely would be a different
-- decision, and a wrong one: it would understate the order count.
--
-- This is a business rule, so it is tested rather than trusted.

select order_key, status, revenue
from {{ source('gold', 'fact_orders') }}
where status in ('cancelled', 'returned', 'pending')
  and revenue <> 0
