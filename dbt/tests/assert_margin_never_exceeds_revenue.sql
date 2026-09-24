-- Margin above revenue means the cost went negative somewhere.
-- Silver already rejects a negative unit_cost, so this test guards the JOIN:
-- a product matched to the wrong cost would show up here.

select product_key, product_name, revenue, margin
from {{ ref('mart_product_performance') }}
where margin > revenue + 0.01
