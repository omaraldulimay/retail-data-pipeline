select
    e.order_id,
    e.tracking_number,
    e.carrier,
    e.event,
    e.event_at,
    e.return_reason,
    e.file_name
from {{ ref('stg_logistics_events') }} as e
where (e.file_name || ':' || e.line_no) not in (
    select record_key from {{ ref('dq_rejected_rows') }} where source = 'logistics'
)
