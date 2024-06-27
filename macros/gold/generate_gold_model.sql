{% macro generate_gold_model(table, dynamic_sql_query) %}
    {%- set config = model['config'] -%}
    {% set source_stream = config['source_stream'] %}

    {% if snapshot_is_incremental() and source_stream != 'full_extract' %}
        {% set max_ingestion_time_query %}
            select max(edp_effective_dttm) from {{ this }}
        {% endset %}

        {% set audit_ingestion_time_max = run_query(max_ingestion_time_query) %}
        {% if execute %}
            {% set audit_ingestion_time_max_result = audit_ingestion_time_max.columns[0][0] %}
        {% endif %}

        {# {{ log('audit_ingestion_time_max'~audit_ingestion_time_max_result, info=true) }} #}
        {% if 'where' in dynamic_sql_query | lower %}
            {{ log('The where clause is detected in the sql_query', info=true) }}
            {{ dynamic_sql_query }}
            and (audit_ingestion_time > '{{ audit_ingestion_time_max_result }}')
        {% else %}
            {{ log('The where clause is not detected in the sql_query', info=true) }}
            {{ dynamic_sql_query}}
            WHERE (audit_ingestion_time > '{{ audit_ingestion_time_max_result }}')
        {% endif %}
{% else %}
    {# the macro is used to create the B3 model of bronze table #}
    {{ dynamic_sql_query }}

    {% endif %}

{% endmacro %}