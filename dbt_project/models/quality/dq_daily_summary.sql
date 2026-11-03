select
    observed_on,
    source,
    rule,
    count(*) as rejected_rows
from {{ ref('dq_rejected_rows') }}
group by all
