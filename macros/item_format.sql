{%- set category_suffix_map = {
    'Beverages': 'BEV',
    'Butchers': 'BUT',
    'Computers and electric accessories': 'CEA',
    'Electric household essentials': 'EHE',
    'Food': 'FOOD',
    'Furniture': 'FUR',
    'Milk Products': 'MILK',
    'Patisserie': 'PAT'
} -%}

{% macro clean_item_by_category(item_col, category_col, price_col, start_price=5.0, step=1.5) %}
  CASE
    {% for category, suffix in category_suffix_map.items() %}
      WHEN {{ item_col }} IS NULL AND {{ category_col }} = '{{ category }}'
        THEN CONCAT(
          'Item_',
          CAST(ROUND((COALESCE({{ price_col }}, 0) - {{ start_price }}) / {{ step }} + 1) AS STRING),
          '_{{ suffix }}'
        )
    {% endfor %}
    ELSE {{ item_col }}
  END
{% endmacro %}