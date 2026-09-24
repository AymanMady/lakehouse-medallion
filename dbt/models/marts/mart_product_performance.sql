-- How each product performs, and where it ranks inside its category.
--
-- The rank is computed here rather than in the BI tool on purpose: two
-- dashboards computing "top 10" their own way is how two teams end up
-- disagreeing about which product sells best.

with per_product as (
    select
        product_key,
        max(product_name) as product_name,
        max(category)     as category,
        max(subcategory)  as subcategory,
        max(supplier)     as supplier,
        count(distinct order_key)    as orders,
        count(distinct customer_key) as customers,
        sum(quantity)                as units_sold,
        round(sum(revenue), 2)       as revenue,
        round(sum(margin), 2)        as margin,
        round(avg(discount_pct), 4)  as average_discount
    from {{ ref('int_order_lines') }}
    where product_key is not null
    group by product_key
)

select
    product_key,
    product_name,
    category,
    subcategory,
    supplier,
    orders,
    customers,
    units_sold,
    revenue,
    margin,
    average_discount,
    case when revenue > 0 then round(100 * margin / revenue, 2) end as margin_pct,
    case when units_sold > 0 then round(revenue / units_sold, 2) end as revenue_per_unit,
    rank() over (partition by category order by revenue desc) as rank_in_category,
    rank() over (order by revenue desc)                       as rank_overall
from per_product
