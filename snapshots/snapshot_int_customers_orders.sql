{% snapshot snapshot_int_hospital_encounters %}

{{
    config(
        unique_key='Surrogate_Key',  
        strategy='timestamp',                 
        updated_at='updated_timestamp'                 
    )
}}

SELECT
    surrogate_key,
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    city,
    state,
    order_id,
    order_date,
    ship_date,
    order_status,
    total_amount,
    discount,
    created_timestamp,  -- Timestamp indicating when this record was first created
    updated_timestamp   -- Timestamp indicating when this record was last updated
FROM {{ ref('int_customers_orders') }}

{% endsnapshot %}
