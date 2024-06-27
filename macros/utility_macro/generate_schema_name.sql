{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {% elif target.schema == "dev" %}
        {{ custom_schema_name | trim }}
    
    {% elif target.schema == "test" %}
        {{ custom_schema_name | trim }}
    
    {% elif target.schema == "prod" %}
        {{ custom_schema_name | trim }}

    {%- else -%}

        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}