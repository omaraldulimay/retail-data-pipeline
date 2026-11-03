-- The shop sends a new version of an order when it changes (e.g. a cancellation).
-- Only the latest version counts.
with ranked as (
    select
        *,
        row_number() over (
            partition by order_id order by updated_at desc, _loaded_at desc
        ) as version_rank
    from {{ source('raw', 'shop_orders') }}
)

select
    order_id,
    (payload ->> 'created_at')::timestamp as created_at,
    (payload ->> 'updated_at')::timestamp as updated_at,
    payload ->> 'status' as status,
    payload ->> 'channel' as channel,
    payload ->> 'customer_id' as customer_id,
    payload ->> 'country' as country,
    payload -> 'lines' as lines
from ranked
where version_rank = 1
