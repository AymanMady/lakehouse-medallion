-- The grain of the mart: exactly one row per (month, country).
--
-- A duplicate here means the aggregation lost part of its GROUP BY, and every
-- total built on top would be doubled. Checking the grain is the cheapest
-- test there is, and the one that catches the most expensive mistakes.

select year_month, country, count(*) as rows_for_the_key
from {{ ref('mart_monthly_revenue') }}
group by year_month, country
having count(*) > 1
