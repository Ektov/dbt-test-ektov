{{ config(materialized='view') }}

select
    order_id::integer as order_id,
    user_id::integer as user_id,
    order_date::date as order_date,
    lower(trim(order_status))::varchar(50) as order_status,
    total_amount::numeric(12, 2) as total_amount
from {{ ref('raw_orders') }}
