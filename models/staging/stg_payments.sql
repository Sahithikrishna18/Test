{{ config(
    materialized='table'
) }}

SELECT
    payment_id,
    order_id,
    payment_date,
    payment_method,
    amount,
    currency,
    status
FROM {{ ref('raw_payments') }}