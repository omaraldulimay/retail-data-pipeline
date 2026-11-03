-- How many days the current stock lasts at the pace of the last 28 days.
with latest_snapshot as (
    select max(snapshot_date) as snapshot_date
    from {{ ref('stg_stock_snapshots') }}
),

stock as (
    select s.sku, s.on_hand, s.snapshot_date
    from {{ ref('stg_stock_snapshots') }} as s
    inner join latest_snapshot using (snapshot_date)
    where s.on_hand >= 0
),

demand as (
    select sku, sum(quantity) / 28.0 as avg_daily_units
    from {{ ref('fct_order_lines') }}
    where status <> 'cancelled'
        and order_date > (select snapshot_date from latest_snapshot) - 28
    group by sku
)

select
    stock.snapshot_date,
    stock.sku,
    p.product_name,
    p.category,
    stock.on_hand,
    round(coalesce(demand.avg_daily_units, 0), 2) as avg_daily_units,
    case
        when coalesce(demand.avg_daily_units, 0) = 0 then null
        else round(stock.on_hand / demand.avg_daily_units, 1)
    end as days_of_cover,
    case
        when stock.on_hand = 0 then 'ausverkauft'
        when demand.avg_daily_units > 0 and stock.on_hand / demand.avg_daily_units < 7 then 'kritisch'
        when demand.avg_daily_units > 0 and stock.on_hand / demand.avg_daily_units < 14 then 'knapp'
        else 'ok'
    end as coverage_status
from stock
inner join {{ ref('stg_products') }} as p using (sku)
left join demand using (sku)
