SELECT *
FROM {{ ref('int_products_suppliers') }}
WHERE stock <= 0
