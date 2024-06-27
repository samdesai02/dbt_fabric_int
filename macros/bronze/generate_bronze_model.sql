{% macro generate_bronze_B3_model(source_name, table) %}
-- set parameters
{%- set config = model['config'] -%}
{% set source_stream = config['source_stream'] %}
{% set schema_enforcement_switch = config.get('schema_enforcement', false)%}
{% set enforced_columns = config['enforced_columns'] %}
{% set enforced_column_data_type = config['enforced_column_data_type'] %}

-- if schema_enforcement_switch is true then get the cols and dtype from config dictionary else select all cols from sources
{% if schema_enforcement_switch == true %}
    {# the macro is used to create the B3 model of a raw table #}

    select {%- for (col, dtype) in zip(enforced_columns, enforced_column_data_type) %}

    CAST("{{col}}" as {{dtype}}) as "{{col}}"{%- if not loop.last %},{{ '\n  ' }}{% endif %}

    {%- endfor %}

    from {{source(source_name, table)}}

{% else %}
    select *
    from {{source(source_name, table)}}
{% endif %}

{% if snapshot_is_incremental() and source_stream != 'full_extract' %}

where audit_ingestion_time

 > (select max(audit_ingestion_time) from {{ this }})

{% endif %}

{% endmacro %}
-- why only enforced columns are taken from source if schema enforcement is true