-- The daily mart must not invent or lose revenue.
--
-- A tolerance of 1 unit, not exact equality: fact_orders rounds the sum ONCE
-- while fact_order_items rounds EACH line, so the two grains legitimately
-- differ by a few cents over thousands of lines. Demanding exact equality
-- would make this test fail for a reason that is not a bug - and a test that
-- cries wolf is a test people switch off.

with mart as (
    select round(sum(revenue), 2) as total from {{ ref('mart_daily_sales') }}
),
facts as (
    select round(sum(revenue), 2) as total from {{ source('gold', 'fact_order_items') }}
)
select mart.total as mart_total, facts.total as fact_total
from mart cross join facts
where abs(mart.total - facts.total) > 1
