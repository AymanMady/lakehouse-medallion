-- RFM segmentation: Recency, Frequency, Monetary.
--
-- The classic way to turn an order history into something a marketing team can
-- act on. Each customer is scored 1 to 5 on three axes, and the combination
-- gives the segment.
--
-- Recency is scored BACKWARDS: a small number of days since the last order is
-- a GOOD sign, so it has to become a high score. Getting that inversion wrong
-- is the most common bug in an RFM model, and it silently labels the best
-- customers as churned.

with per_customer as (
    select
        customer_key,
        max(country)       as country,
        max(segment)       as segment,
        max(signup_cohort) as signup_cohort,
        max(date_day)      as last_order_date,
        min(date_day)      as first_order_date,
        count(distinct order_key) as frequency,
        round(sum(revenue), 2)    as monetary,
        round(sum(margin), 2)     as margin
    from {{ ref('int_order_lines') }}
    where customer_key is not null
      and is_revenue
    group by customer_key
),

scored as (
    select
        *,
        datediff(current_date(), last_order_date) as recency_days,
        -- ntile(5) splits the population into five equal buckets. Fixed
        -- thresholds would need re-tuning every time the business grows;
        -- quintiles adapt on their own.
        6 - ntile(5) over (order by datediff(current_date(), last_order_date)) as r_score,
        ntile(5) over (order by frequency) as f_score,
        ntile(5) over (order by monetary)  as m_score
    from per_customer
)

select
    customer_key,
    country,
    segment,
    signup_cohort,
    first_order_date,
    last_order_date,
    recency_days,
    frequency,
    monetary,
    margin,
    r_score,
    f_score,
    m_score,
    r_score + f_score + m_score as rfm_total,
    case
        when r_score >= 4 and f_score >= 4 and m_score >= 4 then 'champion'
        when r_score >= 4 and f_score >= 3                  then 'loyal'
        when r_score >= 4                                   then 'promising'
        when r_score = 3                                    then 'needs_attention'
        when r_score <= 2 and m_score >= 4                  then 'at_risk_valuable'
        else 'churned'
    end as rfm_segment
from scored
