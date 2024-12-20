{{ config(
    materialized='ephemeral',
    unique_key='surrogate_key'
) }}

SELECT
    -- Generate a surrogate key combining product_id and supplier_id
    MD5(CONCAT(p.product_id, '-', s.supplier_id)) AS surrogate_key,
    p.product_id,
    p.product_name,
    p.category,
    p.price,
    p.stock,
    p.created_at,
    p.supplier AS supplier_name,
    s.supplier_id,
    s.contact_person,
    s.email AS supplier_email,
    s.phone AS supplier_phone,
    s.address AS supplier_address,
    s.country
FROM {{ ref('stg_products') }} p
JOIN {{ ref('stg_suppliers') }} s
    ON p.supplier = s.supplier_name
WHERE p.stock > 0
