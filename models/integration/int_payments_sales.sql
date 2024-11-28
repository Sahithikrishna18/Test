{{ config(
    materialized='table',
    unique_key='surrogate_key'
) }}

WITH sales_data AS (
    SELECT
        s.sale_id,
        s.order_id,
        s.product_id,
        s.quantity,
        s.sale_price,
        s.tax,
        s.discount
    FROM {{ ref('stg_sales') }} s
),

payment_data AS (
    SELECT
        p.payment_id,
        p.order_id,
        p.payment_date,
        p.payment_method,
        p.amount AS payment_amount,
        p.currency,
        p.status AS payment_status
    FROM {{ ref('stg_payments') }} p
)

-- Join the Sales and Payments tables on the order_id
SELECT
    MD5(CONCAT(s.sale_id, '-', s.order_id, '-', p.payment_id)) AS surrogate_key,
    
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
    p.payment_amount,
    p.currency,
    p.payment_status
FROM sales_data s
LEFT JOIN payment_data p
    ON s.order_id = p.order_id
