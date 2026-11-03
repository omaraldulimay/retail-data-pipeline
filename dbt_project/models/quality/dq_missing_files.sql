-- The logistics partner sends one file per day. A day without a file means data is missing.
with loaded as (
    select strptime(regexp_extract(file_name, '\d{4}-\d{2}-\d{2}'), '%Y-%m-%d')::date as file_date
    from {{ source('meta', 'loaded_files') }}
),

expected as (
    select unnest(generate_series(min(file_date), max(file_date), interval 1 day))::date as file_date
    from loaded
)

select
    expected.file_date as missing_date,
    'versand_' || strftime(expected.file_date, '%Y-%m-%d') || '.csv' as expected_file
from expected
left join loaded on loaded.file_date = expected.file_date
where loaded.file_date is null
