{% macro format_currency(column_name) %}
    round(cast({{ column_name }} as numeric(16, 2)), 2)
{% endmacro %}
