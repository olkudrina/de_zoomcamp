{{
    config(
        materialized='table'
    )
}}

with quarterly_revenue as (
  select service_type, year, quarter, year_quarter, sum(total_amount) as revenue
  from {{ ref('fact_trips') }}
  group by 1, 2, 3, 4
)
select cur.service_type, cur.year, cur.quarter,
cur.revenue as current_revenue, prev.revenue as previous_revenue,
round(((cur.revenue - prev.revenue) / prev.revenue), 4) as yoy_change
from quarterly_revenue cur
left join quarterly_revenue prev
  on cur.service_type = prev.service_type
  and cur.quarter = prev.quarter
  and cur.year = prev.year + 1
order by 1