{{ config(
    materialized='incremental',
    unique_key='surrogate_key'
) }}

WITH joined_data AS (
    SELECT
        -- Generate surrogate key
        md5(CONCAT(c.customer_id, '-', o.order_id)) AS surrogate_key,
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
        COALESCE({{ this }}.created_timestamp, CURRENT_TIMESTAMP()) AS created_timestamp,
        CASE
            WHEN {{ this }}.surrogate_key IS NULL
            OR {{ this }}.customer_id != c.customer_id
            OR {{ this }}.order_id != o.order_id
            OR {{ this }}.order_date != o.order_date
            OR {{ this }}.ship_date != o.ship_date
            OR {{ this }}.order_status != o.order_status
            OR {{ this }}.total_amount != o.total_amount
            OR {{ this }}.discount != o.discount
            OR {{ this }}.first_name != c.first_name
            OR {{ this }}.last_name != c.last_name
            OR {{ this }}.email != c.email
            OR {{ this }}.phone != c.phone
            OR {{ this }}.city != c.city
            OR {{ this }}.state != c.state
            THEN CURRENT_TIMESTAMP()
            ELSE {{ this }}.updated_timestamp
        END AS updated_timestamp
    FROM
        {{ ref('stg_customers') }} c
    JOIN
        {{ ref('stg_orders') }} o ON c.customer_id = o.customer_id
    LEFT JOIN
        {{ this }} ON {{ this }}.surrogate_key = md5(CONCAT(c.customer_id, '-', o.order_id))
)

SELECT * FROM joined_data

{% if is_incremental() %}
WHERE surrogate_key NOT IN (SELECT surrogate_key FROM {{ this }})
OR surrogate_key IN (SELECT surrogate_key FROM {{ this }} WHERE updated_timestamp < CURRENT_TIMESTAMP())
{% endif %}
