-- Quarantine: every row that breaks a rule, with the rule it broke.
-- Clean models exclude these rows; the dashboard shows them so someone can fix the source.
with order_line_issues as (
    select
        'shop' as source,
        order_line_id as record_key,
        case
            when quantity <= 0 then 'Menge kleiner oder gleich 0'
            when unit_price < 0 then 'Negativer Preis'
            else 'Unbekannter Artikel'
        end as rule,
        'SKU ' || sku || ', Menge ' || quantity || ', Preis ' || unit_price as details,
        created_at::date as observed_on
    from {{ ref('stg_order_lines') }}
    where quantity <= 0
        or unit_price < 0
        or sku not in (select sku from {{ ref('stg_products') }})
),

stock_issues as (
    select
        'warehouse' as source,
        sku || '@' || snapshot_date as record_key,
        'Negativer Bestand' as rule,
        'Bestand ' || on_hand as details,
        snapshot_date as observed_on
    from {{ ref('stg_stock_snapshots') }}
    where on_hand < 0
),

logistics_issues as (
    select
        'logistics' as source,
        file_name || ':' || line_no as record_key,
        case
            when event_at is null then 'Ungültiges Datum'
            when is_duplicate then 'Doppelte Zeile'
            else 'Unbekannte Bestellung'
        end as rule,
        order_id || ' ' || event || ' ' || coalesce(event_time_raw, '') as details,
        coalesce(event_at::date, _loaded_at::date) as observed_on
    from {{ ref('stg_logistics_events') }}
    where event_at is null
        or is_duplicate
        or order_id not in (select order_id from {{ ref('stg_orders') }})
)

select * from order_line_issues
union all
select * from stock_issues
union all
select * from logistics_issues
