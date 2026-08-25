{{ config(materialized='view') }}

select
    product_id::integer as product_id,
    trim(product_name)::varchar(255) as product_name,
    lower(trim(category))::varchar(100) as category,
    price::numeric(12, 2) as price
from {{ ref('raw_products') }}
