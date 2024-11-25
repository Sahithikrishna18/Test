{{ config(
    materialized='table'
) }}

SELECT
    sale_id,
    order_id,
    product_id,
    quantity,
    sale_price,
    tax,
    discount
FROM {{ ref('raw_sales') }}