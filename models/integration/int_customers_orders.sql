{{ config(
    materialized='incremental',
    unique_key='surrogate_key'
) }}

-- Combine both customers and orders data
SELECT
    MD5(CONCAT(c.customer_id, '-', o.order_id)) AS surrogate_key,
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.phone,
    c.city,
    c.state,
    o.order_id,
    o.order_date,
    o.ship_date,
    o.order_status,
    o.total_amount,
    o.discount,
    CURRENT_TIMESTAMP() AS created_timestamp,
    CURRENT_TIMESTAMP() AS updated_timestamp
FROM {{ref('stg_customers')}} c
JOIN {{ ref('stg_orders') }} o
    ON c.customer_id = o.customer_id
{% if is_incremental() %}
    WHERE updated_timestamp > (SELECT MAX(updated_timestamp) FROM {{ this }})
{% endif %}
