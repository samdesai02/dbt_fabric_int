{% macro check_table_row_cnt(target) %}
    -- count query set to model_sql
    {% set model_sql %}
        select count(*) as row_cnt
        from ({{target}}) as t
    {% endset %}
    --  execute query model_sql using run_query function
    {% set result = run_query(model_sql) %}
    -- get row count
    {% if execute %}
        {% set row_cnt = result.columns[0].values()[0] %}
    {% else %}
        {% set row_cnt = none %}
    {% endif %}
    --  return count
    {{ return(row_cnt) }}

{% endmacro %}