{{ config(
    materialized='ephemeral',
    unique_key='surrogate_key'
) }}

SELECT
    MD5(CONCAT(p.product_id, '-', s.supplier_id)) AS surrogate_key,
    p.product_id,
    p.product_name,
    p.category,
    p.price,
    p.stock,
    p.supplier AS supplier_name,
    s.email AS supplier_email,
    s.phone AS supplier_phone,
    s.address AS supplier_address,
    s.country
FROM stg_products p
JOIN stg_suppliers s
    ON p.supplier = s.supplier_name
WHERE p.stock > 0
