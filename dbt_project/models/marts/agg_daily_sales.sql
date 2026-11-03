select
    order_date,
    channel,
    count(*) as orders,
    sum(units) as units,
    sum(revenue) as revenue,
    sum(margin) as margin,
    count(*) filter (where is_returned) as returned_orders
from {{ ref('fct_orders') }}
where status <> 'cancelled'
group by all
