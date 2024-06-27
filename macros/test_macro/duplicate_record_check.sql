{% test duplicate_record_check_active_record_only(model,
                                 column_name
                                 ) %}
    with distinct_rows as (
    select
        {{column_name}}
    from {{ model }}
    WHERE
    {{column_name}} not like '{{ like_pattern }}'

),
validation_errors as (

    select
        *
    from
        grouped_expression

)

select *
from validation_errors
{%- endtest %}