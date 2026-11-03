select
    l.order_line_id,
    l.order_id,
    l.line_no,
    l.created_at,
    l.created_at::date as order_date,
    l.status,
    l.channel,
    l.country,
    l.sku,
    p.product_name,
    p.category,
    l.quantity,
    l.unit_price,
    l.quantity * l.unit_price as revenue,
    l.quantity * p.unit_cost as cost,
    l.quantity * (l.unit_price - p.unit_cost) as margin
from {{ ref('stg_order_lines') }} as l
inner join {{ ref('stg_products') }} as p on p.sku = l.sku
where l.order_line_id not in (
    select record_key from {{ ref('dq_rejected_rows') }} where source = 'shop'
)
