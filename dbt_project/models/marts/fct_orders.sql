-- One row per order with its value and delivery story.
-- Delivery promise: the parcel arrives within {{ var('delivery_promise_days', 5) }} days of the order.
with lines as (
    select
        order_id,
        sum(quantity) as units,
        sum(revenue) as revenue,
        sum(margin) as margin,
        count(*) as line_count
    from {{ ref('fct_order_lines') }}
    group by order_id
),

shipping as (
    select
        order_id,
        any_value(carrier) as carrier,
        min(event_at) filter (where event = 'VERSENDET') as shipped_at,
        min(event_at) filter (where event = 'ZUGESTELLT') as delivered_at,
        min(event_at) filter (where event = 'RETOURE_EINGANG') as returned_at,
        any_value(return_reason) filter (where event = 'RETOURE_EINGANG') as return_reason
    from {{ ref('fct_shipment_events') }}
    group by order_id
)

select
    o.order_id,
    o.created_at,
    o.created_at::date as order_date,
    o.status,
    o.channel,
    o.country,
    o.customer_id,
    coalesce(lines.units, 0) as units,
    coalesce(lines.revenue, 0) as revenue,
    coalesce(lines.margin, 0) as margin,
    s.carrier,
    s.shipped_at,
    s.delivered_at,
    s.returned_at,
    s.return_reason,
    o.created_at::date + {{ var('delivery_promise_days', 5) }} as promised_by,
    s.returned_at is not null as is_returned,
    case
        when s.delivered_at is null then null
        else s.delivered_at::date <= o.created_at::date + {{ var('delivery_promise_days', 5) }}
    end as is_on_time,
    date_diff('day', o.created_at, s.delivered_at) as days_to_deliver
from {{ ref('stg_orders') }} as o
left join lines on lines.order_id = o.order_id
left join shipping as s on s.order_id = o.order_id
