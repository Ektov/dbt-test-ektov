{{ config(materialized='table') }}

-- Deduplicate intentional seed duplicates so the customer grain stays unique
with users_deduped as (
    select distinct on (user_id)
        user_id,
        email,
        country,
        status,
        created_at
    from {{ ref('stg_users') }}
    order by user_id, created_at nulls last
),

customer_orders as (
    select
        u.user_id,
        u.email,
        u.country,
        u.status as user_status,
        u.created_at as customer_since,
        o.order_id,
        o.order_date,
        o.order_status,
        o.total_amount
    from users_deduped as u
    left join {{ ref('stg_orders') }} as o
        on u.user_id = o.user_id
)

select
    user_id,
    email,
    country,
    user_status,
    customer_since,
    min(order_date) filter (where order_id is not null) as first_order_date,
    max(order_date) filter (where order_id is not null) as last_order_date,
    count(distinct order_id) as total_orders,
    coalesce(sum(total_amount) filter (where order_status = 'completed'), 0)::numeric(12, 2) as total_spent,
    coalesce(
        sum(total_amount) filter (where order_status = 'completed')
            / nullif(count(distinct order_id) filter (where order_status = 'completed'), 0),
        0
    )::numeric(12, 2) as avg_order_value
from customer_orders
group by
    user_id,
    email,
    country,
    user_status,
    customer_since
