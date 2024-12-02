SELECT *
FROM {{ ref('int_customers_orders') }}
WHERE order_date > CURRENT_TIMESTAMP or ship_date > CURRENT_TIMESTAMP
