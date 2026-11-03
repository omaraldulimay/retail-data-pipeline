select
    date_trunc('week', shipped_at)::date as week,
    carrier,
    count(*) as shipped_orders,
    count(delivered_at) as delivered_orders,
    avg(is_on_time::integer) as on_time_rate,
    avg(days_to_deliver) as avg_days_to_deliver
from {{ ref('fct_orders') }}
where shipped_at is not null
group by all
