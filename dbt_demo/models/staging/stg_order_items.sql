{{ config(materialized='view') }}

select
    item_id::integer as item_id,
    order_id::integer as order_id,
    product_id::integer as product_id,
    quantity::integer as quantity,
    unit_price::numeric(12, 2) as unit_price
from {{ ref('raw_order_items') }}
