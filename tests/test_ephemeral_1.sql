SELECT *
FROM {{ ref('int_products_suppliers') }}
WHERE supplier_email NOT LIKE '%_@__%.__%'
