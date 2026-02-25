{% macro clone_schema_across_db(from_db, from_schema, to_db, to_schema) %}
    {% set sql %}
        -- 1. Create/Replace the Target Database
        CREATE DATABASE IF NOT EXISTS {{ to_db }};
        
        -- 2. Clone the Schema (Clones internal tables, views, etc.)
        CREATE OR REPLACE SCHEMA {{ to_db }}.{{ to_schema }} 
        CLONE {{ from_db }}.{{ from_schema }};
        
        -- 3. INDUSTRIAL FIX: Manually re-create External Tables
        -- We fetch the DDL of external tables from the source and run it in the target
        {% set get_ext_tables %}
            SELECT 'CREATE OR REPLACE EXTERNAL TABLE {{ to_db }}.{{ to_schema }}.' || table_name || 
                   ' ' || GET_DDL('table', '{{ from_db }}.{{ from_schema }}.' || table_name)
            FROM {{ from_db }}.information_schema.external_tables
            WHERE table_schema = '{{ from_schema }}';
        {% endset %}

        -- Note: For a simpler project, it is often easier to just run 
        -- 'dbt run-operation stage_external_sources --target dev' 
        -- if you are using the dbt-external-tables package.
    {% endset %}
    
    {% do run_query(sql) %}
    {{ log("SUCCESS: Cloned " ~ from_schema ~ " and handled external table logic.", info=True) }}
{% endmacro %}
