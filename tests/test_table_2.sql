SELECT *
FROM {{ ref('int_payments_sales') }}
WHERE payment_date > CURRENT_DATE
