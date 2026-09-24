-- The trap this model is most likely to fall into.
--
-- Recency is scored backwards: FEWER days since the last order must give a
-- HIGHER r_score. Get the inversion wrong and the model quietly labels your
-- best customers as churned - with no error, no NULL, nothing to notice.
--
-- The check: the customer who ordered most recently must not score lower than
-- the one who ordered longest ago.

with bounds as (
    select
        min(recency_days) as most_recent,
        max(recency_days) as least_recent
    from {{ ref('mart_customer_rfm') }}
),
scores as (
    select
        max(case when r.recency_days = b.most_recent  then r.r_score end) as score_recent,
        max(case when r.recency_days = b.least_recent then r.r_score end) as score_old
    from {{ ref('mart_customer_rfm') }} r
    cross join bounds b
)
select * from scores where score_recent <= score_old
