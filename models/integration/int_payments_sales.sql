{{ config(
    materialized='table'
) }}

WITH staged_payments AS (
    SELECT
        payment_id,
        order_id,
        payment_date,
        payment_method,
        amount,
        currency,
        status
    FROM {{ ref('stg_payments') }}
),

staged_sales AS (
    SELECT
        sale_id,
        order_id,
        product_id,
        quantity,
        sale_price,
        tax,
        discount
    FROM {{ ref('stg_sales') }}
)

SELECT
    s.sale_id,
    s.order_id,
    s.product_id,
    s.quantity,
    s.sale_price,
    s.tax,
    s.discount,
    p.payment_id,
    p.payment_date,
    p.payment_method,
    p.amount,
    p.currency,
    p.status
FROM staged_sales s
LEFT JOIN staged_payments p
    ON s.order_id = p.order_id;
