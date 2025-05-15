{% macro item_price_pattern(item_number) %}
    5.0 + (({{ item_number }} - 1) * 1.5)
{% endmacro %}