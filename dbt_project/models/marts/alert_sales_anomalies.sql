-- Days whose revenue differs a lot from the same weekday in the 4 weeks before.
with daily as (
    select order_date, sum(revenue) as revenue
    from {{ ref('agg_daily_sales') }}
    group by order_date
),

compared as (
    select
        d.order_date,
        d.revenue,
        avg(p.revenue) as expected_revenue,
        count(p.order_date) as weeks_compared
    from daily as d
    left join daily as p
        on p.order_date in (
            d.order_date - 7, d.order_date - 14, d.order_date - 21, d.order_date - 28
        )
    group by d.order_date, d.revenue
),

scored as (
    select
        *,
        round((revenue / expected_revenue - 1) * 100, 1) as deviation_pct
    from compared
    where weeks_compared = 4 and expected_revenue > 0
)

select
    order_date,
    revenue,
    round(expected_revenue, 2) as expected_revenue,
    deviation_pct,
    case
        when deviation_pct <= -30 then 'Einbruch'
        when deviation_pct >= 50 then 'Ausreißer nach oben'
    end as direction
from scored
where deviation_pct <= -30 or deviation_pct >= 50
