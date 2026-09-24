-- Revenue per day.
--
-- Built from dim_date on the LEFT, not from the facts. A day with no order
-- still has to appear, with a zero: deriving the calendar from the facts would
-- silently skip it, and a flat line on a chart would look like missing data
-- rather than a quiet day.

with lines as (
    select * from {{ ref('int_order_lines') }}
),

per_day as (
    select
        date_key,
        count(distinct order_key)                    as orders,
        count(*)                                     as order_lines,
        count(distinct customer_key)                 as active_customers,
        sum(quantity)                                as units_sold,
        round(sum(revenue), 2)                       as revenue,
        round(sum(margin), 2)                        as margin,
        round(sum(gross_amount - net_amount), 2)     as discount_given
    from lines
    group by date_key
)

select
    d.date_key,
    d.date_day,
    d.year,
    d.quarter,
    d.year_month,
    d.day_name,
    d.is_weekend,
    coalesce(p.orders, 0)           as orders,
    coalesce(p.order_lines, 0)      as order_lines,
    coalesce(p.active_customers, 0) as active_customers,
    coalesce(p.units_sold, 0)       as units_sold,
    coalesce(p.revenue, 0)          as revenue,
    coalesce(p.margin, 0)           as margin,
    coalesce(p.discount_given, 0)   as discount_given,
    -- Computed from the raw sums, never from an already rounded total: a
    -- double rounding is how a one-cent gap appears that nobody can explain.
    case when coalesce(p.orders, 0) > 0
         then round(coalesce(p.revenue, 0) / p.orders, 2)
    end as average_order_value

from {{ source('gold', 'dim_date') }} as d
left join per_day as p on d.date_key = p.date_key
