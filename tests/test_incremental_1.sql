SELECT *
FROM {{ ref('int_customers_orders') }}
WHERE updated_timestamp < created_timestamp
