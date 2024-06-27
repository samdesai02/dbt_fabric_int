{% macro add_table_prefix_to_scd_expr(table_alias, unique_key) %}
-- 'a' is redundant
{% set a = "compliance_item_id||'-'||project_contact_id||'-'||safasf" %}
-- split unique_key with delimiter "+'-'+" and create list
{% set split_unique_keys = unique_key.split("+'-'+") %}
--  Initialise
{% set add_prefix_unique_keys = [] %}
--  concate string. table_alias.field_name
{% for field in split_unique_keys %}
    {% do add_prefix_unique_keys.append(table_alias~'.'~field) %}
{% endfor %}
-- join all the keys using delimiter "+'-'+"
{% set add_prefix_unique_keys_str = add_prefix_unique_keys | join("+'-'+") %}
--  return joined key
{{ return(add_prefix_unique_keys_str) }}

{% endmacro %}