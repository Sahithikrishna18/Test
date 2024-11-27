{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

WITH staged_customers AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        city,
        state
    FROM {{ ref('stg_customers') }}
),

staged_orders AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        ship_date,
        order_status,
        total_amount,
        discount,
        created_at,
        updated_at
    FROM {{ ref('stg_orders') }}
)

SELECT
    o.order_id,
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.phone,
    c.city,
    c.state,
    o.order_date,
    o.ship_date,
    o.order_status,
    o.total_amount,
    o.discount
FROM staged_orders o
LEFT JOIN staged_customers c
    ON o.customer_id = c.customer_id
WHERE o.updated_at >= (SELECT COALESCE(MAX(updated_at), '1900-01-01')
FROM {{ this }}
)