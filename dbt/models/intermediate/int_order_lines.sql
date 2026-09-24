-- One order line, with everything a mart needs already attached.
--
-- Ephemeral on purpose: dbt compiles it as a CTE inside each mart that uses
-- it. No table is created, so this step costs nothing in storage and can never
-- go stale behind the marts that depend on it.
--
-- A LEFT join on the dimensions, not an INNER one. An inner join here would
-- silently drop a line whose product is missing from the dimension, and the
-- revenue would fall without anything reporting it. With a LEFT join the line
-- survives, the attribute is NULL, and the gap is visible.

select
    i.order_item_key,
    i.order_key,
    i.product_key,
    i.customer_key,
    i.date_key,
    d.date_day,
    d.year_month,
    d.year,
    d.quarter,
    i.status,
    i.currency,
    i.quantity,
    i.unit_price,
    i.discount_pct,
    i.gross_amount,
    i.net_amount,
    i.cost_amount,
    i.is_revenue,
    i.revenue,
    i.margin,
    c.country,
    c.segment,
    c.signup_cohort,
    p.product_name,
    p.category,
    p.subcategory,
    p.supplier

from {{ source('gold', 'fact_order_items') }} as i
left join {{ source('gold', 'dim_date') }}     as d on i.date_key     = d.date_key
left join {{ source('gold', 'dim_customer') }} as c on i.customer_key = c.customer_key
left join {{ source('gold', 'dim_product') }}  as p on i.product_key  = p.product_key
