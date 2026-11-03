select
    sku,
    payload ->> 'name' as product_name,
    payload ->> 'category' as category,
    payload ->> 'color' as color,
    (payload ->> 'price')::decimal(10, 2) as list_price,
    (payload ->> 'cost')::decimal(10, 2) as unit_cost
from {{ source('raw', 'shop_products') }}
