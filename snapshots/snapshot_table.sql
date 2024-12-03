{% snapshot snapshot_int_payments_sales %}

{{
    config(
        database='Analytics',
        schema='dbt_snalluri',
        unique_key='Surrogate_Key',  
        strategy='timestamp',                 
        updated_at='updated_timestamp'                 
    )
}}

select * from {{ ref('int_payments_sales') }}

{% endsnapshot %}
