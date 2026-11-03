-- Which SKUs to reorder, by when and how much. Only SKUs that need action are listed.
with params as (
    select
        {{ var('supplier_lead_time_days') }} as lead_days,
        {{ var('safety_stock_days') }} as safety_days,
        {{ var('order_cover_days') }} as cover_days
),

base as (
    select
        c.*,
        params.lead_days,
        params.safety_days,
        params.cover_days
    from {{ ref('agg_stock_coverage') }} as c
    cross join params
    where c.days_of_cover is not null
),

scored as (
    select
        *,
        cast(snapshot_date + cast(floor(days_of_cover) as integer) as date) as stockout_date,
        case
            when days_of_cover <= lead_days + safety_days then 'jetzt bestellen'
            when days_of_cover <= lead_days + safety_days + 7 then 'bald bestellen'
        end as priority
    from base
)

select
    snapshot_date,
    sku,
    product_name,
    category,
    on_hand,
    avg_daily_units,
    days_of_cover,
    stockout_date as expected_stockout_date,
    cast(stockout_date - lead_days as date) as reorder_by_date,
    cast(greatest(
        ceil(avg_daily_units * (lead_days + safety_days + cover_days) - on_hand), 0
    ) as integer) as reorder_qty,
    priority
from scored
where priority is not null
