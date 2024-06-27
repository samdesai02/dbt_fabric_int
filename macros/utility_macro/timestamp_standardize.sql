{%- macro standardize_timestamp(timestamp_column, timezone) -%}
    CAST(SWITCHOFFSET({{timestamp_column}}, {{timezone}}*60) AS datetime2(6)) as {{timestamp_column}}
{%- endmacro -%}