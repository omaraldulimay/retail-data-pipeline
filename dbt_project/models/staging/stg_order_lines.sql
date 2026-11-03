-- One row per order line, unchecked. Quality rules are applied in the quality layer.
with orders as (
    select
        *,
        from_json(
            lines, '[{"sku": "VARCHAR", "quantity": "INTEGER", "unit_price": "DECIMAL(10,2)"}]'
        ) as line_list
    from {{ ref('stg_orders') }}
),

exploded as (
    select
        order_id,
        created_at,
        status,
        channel,
        country,
        unnest(line_list) as line,
        generate_subscripts(line_list, 1) as line_no
    from orders
)

select
    order_id || '-' || line_no as order_line_id,
    order_id,
    line_no,
    created_at,
    status,
    channel,
    country,
    line.sku as sku,
    line.quantity as quantity,
    line.unit_price as unit_price
from exploded
