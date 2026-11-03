-- The logistics partner switched from ISO dates to German dates on 2026-06-01,
-- so both formats are accepted. Anything else stays null and is quarantined.
with parsed as (
    select
        file_name,
        line_no,
        trim(order_number) as order_id,
        nullif(trim(tracking_number), '') as tracking_number,
        trim(carrier) as carrier,
        trim(event) as event,
        event_time as event_time_raw,
        coalesce(
            try_strptime(trim(event_time), '%Y-%m-%d %H:%M'),
            try_strptime(trim(event_time), '%d.%m.%Y %H:%M')
        ) as event_at,
        nullif(trim(return_reason), '') as return_reason,
        _loaded_at
    from {{ source('raw', 'logistics_events') }}
)

select
    *,
    -- The partner sometimes repeats a row; the first occurrence counts.
    row_number() over (
        partition by order_id, tracking_number, event, event_at order by file_name, line_no
    ) > 1 as is_duplicate
from parsed
