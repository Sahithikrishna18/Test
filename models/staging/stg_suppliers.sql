{{ config(
    materialized='table'
) }}

SELECT
    supplier_id,
    supplier_name,
    contact_person,
    email,
    phone,
    address,
    country
FROM {{ ref('raw_suppliers') }}