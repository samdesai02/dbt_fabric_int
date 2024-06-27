{% test expect_column_values_to_match_like_pattern_custom(model,
                                 column_name,
                                 like_pattern
                                 ) %}
    with grouped_expression as (
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