{{ config(materialized='view') }}

select
    user_id::integer as user_id,
    nullif(trim(email), '')::varchar(255) as email,
    upper(trim(country))::varchar(10) as country,
    lower(trim(status))::varchar(50) as status,
    created_at::timestamp as created_at
-- Use ref() (not source) so Cosmos/Airflow create seed → staging dependencies
from {{ ref('raw_users') }}
