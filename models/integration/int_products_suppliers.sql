{{ config(
    materialized='ephemeral'
) }}

WITH staged_products AS (
    SELECT
        product_id,
        product_name,
        category,
        price,
        stock,
        created_at,
        supplier
    FROM {{ ref('stg_products') }}
),

staged_suppliers AS (
    SELECT
        supplier_id,
        supplier_name,
        contact_person,
        email,
        phone,
        address,
        country
    FROM {{ ref('stg_suppliers') }}
)

SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.price,
    p.stock,
    p.created_at,
    s.supplier_id,
    s.supplier_name,
    s.contact_person,
    s.email AS supplier_email,
    s.phone AS supplier_phone,
    s.address,
    s.country
FROM staged_products p
LEFT JOIN staged_suppliers s
    ON p.supplier = s.supplier_id;
