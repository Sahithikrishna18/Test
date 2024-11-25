{{ config(
    materialized='table'
) }}

SELECT
    order_id,
    customer_id,
    order_date,
    ship_date,
    order_status,
    total_amount,
    discount
FROM {{ ref('raw_orders') }}