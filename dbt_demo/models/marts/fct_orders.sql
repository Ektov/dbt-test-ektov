{{ config(materialized='table') }}

/*
  Fact table at order-line grain.
  - calculated_total: sum of quantity * unit_price for the whole order (window)
  - is_high_value: calculated_total >= 200
  - category: from product (needed for Elementary dimension_anomalies)
*/

with order_lines as (
    select
        o.order_id,
        oi.item_id,
        o.user_id,
        o.order_date,
        o.order_status,
        o.total_amount as reported_total,
        oi.product_id,
        p.product_name,
        p.category,
        oi.quantity,
        oi.unit_price,
        (oi.quantity::numeric * oi.unit_price)::numeric(12, 2) as line_total
    from {{ ref('stg_orders') }} as o
    inner join {{ ref('stg_order_items') }} as oi
        on o.order_id = oi.order_id
    inner join {{ ref('stg_products') }} as p
        on oi.product_id = p.product_id
)

select
    order_id,
    item_id,
    user_id,
    order_date,
    order_status,
    reported_total,
    product_id,
    product_name,
    category,
    quantity,
    unit_price,
    line_total,
    sum(line_total) over (partition by order_id)::numeric(12, 2) as calculated_total,
    case
        when sum(line_total) over (partition by order_id) >= 200 then true
        else false
    end as is_high_value,
    current_timestamp as updated_at
from order_lines
