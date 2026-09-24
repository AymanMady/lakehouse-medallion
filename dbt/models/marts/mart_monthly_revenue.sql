-- Revenue per month and per country, with the month-over-month change.
--
-- The window function is the reason this belongs in dbt rather than in Spark:
-- a LAG over a partition is one line of SQL, and it is far easier to review
-- here than the equivalent DataFrame code.

with monthly as (
    select
        year_month,
        country,
        count(distinct order_key)    as orders,
        count(distinct customer_key) as customers,
        sum(quantity)                as units_sold,
        round(sum(revenue), 2)       as revenue,
        round(sum(margin), 2)        as margin
    from {{ ref('int_order_lines') }}
    where year_month is not null
      and country is not null
    group by year_month, country
)

select
    year_month,
    country,
    orders,
    customers,
    units_sold,
    revenue,
    margin,
    case when revenue > 0 then round(100 * margin / revenue, 2) end as margin_pct,
    lag(revenue) over (partition by country order by year_month) as revenue_previous_month,
    round(
        revenue - lag(revenue) over (partition by country order by year_month), 2
    ) as revenue_change,
    case
        when lag(revenue) over (partition by country order by year_month) > 0
        then round(
            100 * (revenue - lag(revenue) over (partition by country order by year_month))
            / lag(revenue) over (partition by country order by year_month), 2)
    end as revenue_change_pct
from monthly
