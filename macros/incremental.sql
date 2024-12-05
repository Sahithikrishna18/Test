{% macro generate_surrogate_key(customer_id, order_id) %}
    md5(concat({{ customer_id }}, '-', {{ order_id }}))
{% endmacro %}

