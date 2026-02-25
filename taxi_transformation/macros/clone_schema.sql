{% macro clone_schema_across_db(from_db, from_schema, to_db, to_schema) %}
    {% set sql %}
        CREATE OR REPLACE SCHEMA {{ to_db }}.{{ to_schema }} 
        CLONE {{ from_db }}.{{ from_schema }};
    {% endset %}
    
    {% do run_query(sql) %}
    {{ log("Cloned " ~ from_db ~ "." ~ from_schema ~ " to " ~ to_db ~ "." ~ to_schema, info=True) }}
{% endmacro %}
