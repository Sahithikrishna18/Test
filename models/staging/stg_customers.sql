{{ config(
    materialized='table'
) }}

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    city,
    state
FROM {{ ref('raw_customers') }}
