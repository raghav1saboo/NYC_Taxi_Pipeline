{% macro clean_fare(column_name) %}
    ROUND(CAST(COALESCE({{ column_name }}, 0) AS FLOAT), 2)
{% endmacro %}
