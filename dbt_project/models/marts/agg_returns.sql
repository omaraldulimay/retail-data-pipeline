-- Return rate per category. An order counts for every category it contains.
with order_categories as (
    select distinct l.order_id, l.category
    from {{ ref('fct_order_lines') }} as l
)

select
    c.category,
    count(*) filter (where o.shipped_at is not null) as shipped_orders,
    count(*) filter (where o.is_returned) as returned_orders,
    round(
        count(*) filter (where o.is_returned)
        / nullif(count(*) filter (where o.shipped_at is not null), 0),
        4
    ) as return_rate,
    mode(o.return_reason) filter (where o.is_returned) as top_reason
from order_categories as c
inner join {{ ref('fct_orders') }} as o using (order_id)
where o.status <> 'cancelled'
group by c.category
