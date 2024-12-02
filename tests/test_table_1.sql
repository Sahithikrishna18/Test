SELECT *
FROM {{ ref('int_payments_sales') }}
WHERE discount < 0
