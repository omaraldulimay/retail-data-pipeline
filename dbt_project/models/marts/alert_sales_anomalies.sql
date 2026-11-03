-- Days whose revenue differs a lot from the same weekday in the 4 weeks before.
-- The baseline is the median, so a single peak week does not distort it. Days inside a known
-- calendar event (seed calendar_events) that move in the expected direction are marked
-- is_expected: they stay in the table, but they are not an incident.
with daily as (
    select order_date, sum(revenue) as revenue
    from {{ ref('agg_daily_sales') }}
    group by order_date
),

compared as (
    select
        d.order_date,
        d.revenue,
        median(p.revenue) as expected_revenue,
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
),

flagged as (
    select
        *,
        case
            when deviation_pct <= -30 then 'Einbruch'
            when deviation_pct >= 50 then 'Ausreißer nach oben'
        end as direction
    from scored
    where deviation_pct <= -30 or deviation_pct >= 50
),

with_events as (
    select
        f.order_date,
        f.revenue,
        round(f.expected_revenue, 2) as expected_revenue,
        f.deviation_pct,
        f.direction,
        e.event,
        coalesce(
            e.expected_effect = case f.direction when 'Einbruch' then 'down' else 'up' end,
            false
        ) as is_expected
    from flagged as f
    left join {{ ref('calendar_events') }} as e
        on f.order_date between e.start_date and e.end_date
    qualify row_number() over (partition by f.order_date order by e.start_date desc) = 1
)

select * from with_events
