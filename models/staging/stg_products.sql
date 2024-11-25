{{ config(
    materialized='table'
) }}

SELECT
    product_id,
    product_name,
    category,
    price,
    stock,
    created_at,
    supplier
FROM {{ ref('raw_products') }}