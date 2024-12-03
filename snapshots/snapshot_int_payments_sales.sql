{% snapshot snapshot_int_payments_sales %}

{{
    config(
        database='Analytics',
        schema='dbt_snalluri',
        unique_key='Surrogate_Key',  
        strategy='timestamp',                 
        updated_at='payment_date'                 
    )
}}

select *
from {{ ref('int_payments_sales') }}

{% endsnapshot %}
