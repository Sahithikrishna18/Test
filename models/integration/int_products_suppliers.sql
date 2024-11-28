{{ config(
    materialized='incremental',
    unique_key='SURROGATE_KEY'
) }}

WITH joined_data AS (
    SELECT
        -- Generate surrogate key
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
        s.country,
        COALESCE({{ this }}.created_at, CURRENT_TIMESTAMP()) AS created_at,
        CASE
            WHEN {{ this }}.SURROGATE_KEY IS NULL
              OR {{ this }}.product_name != p.product_name
              OR {{ this }}.category != p.category
              OR {{ this }}.price != p.price
              OR {{ this }}.stock != p.stock
              OR {{ this }}.supplier_name != s.supplier_name
              OR {{ this }}.contact_person != s.contact_person
              OR {{ this }}.supplier_email != s.email
              OR {{ this }}.supplier_phone != s.phone
              OR {{ this }}.supplier_address != s.address
              OR {{ this }}.supplier_country != s.country
            THEN CURRENT_TIMESTAMP()
            ELSE {{ this }}.updated_at
        END AS updated_at
    FROM
        {{ ref('stg_products') }} p
    FULL OUTER JOIN
        {{ ref('stg_suppliers') }} s ON p.supplier = s.supplier_name
    LEFT JOIN
        {{ this }} ON {{ this }}.SURROGATE_KEY = md5(CONCAT(p.product_id, '-', s.supplier_id))
)
SELECT * FROM joined_data
{% if is_incremental() %}
WHERE 
    SURROGATE_KEY NOT IN (SELECT SURROGATE_KEY FROM {{ this }})
    OR SURROGATE_KEY IN (SELECT SURROGATE_KEY FROM {{ this }} WHERE updated_at < CURRENT_TIMESTAMP())
{% endif %}
