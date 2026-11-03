-- No line that reaches the reports may have a non-positive quantity or a negative price.
select order_line_id, quantity, unit_price
from {{ ref('fct_order_lines') }}
where quantity <= 0 or unit_price < 0
