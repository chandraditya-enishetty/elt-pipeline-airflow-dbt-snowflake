{# macros/discounted_amount.sql
   Reusable macro to calculate the discount amount from a line item.
   Using a macro here demonstrates dbt best practice — DRY (Don't Repeat Yourself).
   The same formula used in multiple models should be centralised in a macro.
#}

{% macro discounted_amount(extended_price, discount_percentage, scale=2) %}
    (
        -1 * {{ extended_price }} * {{ discount_percentage }}
    )::decimal(16, {{ scale }})
{% endmacro %}
