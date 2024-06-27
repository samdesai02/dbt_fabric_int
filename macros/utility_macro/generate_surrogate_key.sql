{% macro generate_surrogate_key(column_names) -%}
    {% if column_names is iterable and (column_names | length) > 1 %}
        CONVERT(varchar(256), HashBytes('MD5', concat({%- for col in column_names -%}
            coalesce(cast({{ col }} as varchar(8000) ), ''){% if not loop.last %}, {% endif %}
        {%- endfor -%})), 2)
    {% elif column_names is iterable and (column_names | length) == 1 %}
        CONVERT(varchar(256), HashBytes('MD5', {%- for col in column_names -%}
            coalesce(cast({{ col }} as varchar(8000) ), '')
        {%- endfor -%}), 2)
    {% endif %}
{%- endmacro %}
