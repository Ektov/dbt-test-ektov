select *
from {{ ref('fct_orders') }}
where updated_at > current_date - interval '24 hour'