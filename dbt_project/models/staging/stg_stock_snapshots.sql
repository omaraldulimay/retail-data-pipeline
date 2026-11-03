select
    snapshot_date::date as snapshot_date,
    sku,
    on_hand
from {{ source('raw', 'wms_stock_snapshots') }}
