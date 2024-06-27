{#
{% macro edp_snapshot_merge_sql_full_extract(target, source, insert_cols, strategy) -%}
    {%- set insert_cols_csv = insert_cols | join(', ') -%}

    merge into {{ target }} as DBT_INTERNAL_DEST
    using {{ source }} as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.scd_id = {{ strategy.dest_scd_id}}

    when matched
     and DBT_INTERNAL_DEST.audit_to_timestamp = date_format('9999-12-31T23:59:59.999Z', 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'')::timestamp
     and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
        then update
        set audit_to_timestamp = date_format(DBT_INTERNAL_SOURCE.audit_to_timestamp - INTERVAL 1 milliseconds, 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'')::timestamp, audit_active_ind = 'N',
            audit_last_modified_timestamp = DBT_INTERNAL_SOURCE.audit_last_modified_timestamp
    when not matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
        then insert ({{ insert_cols_csv }})
        values ({{ insert_cols_csv }})


{% endmacro %}
#}

{% macro edp_snapshot_merge_sql_full_extract(target, source, insert_cols, strategy) -%}

  {%- set insert_cols_csv = insert_cols | join(', ') -%}

  {% set target_columns_list = [] %}

  {% for column in insert_cols %}
    {% set target_columns_list = target_columns_list.append("DBT_INTERNAL_SOURCE."+column)  %}
  {% endfor %}

  {%- set target_columns = target_columns_list | join(', ') -%}

  UPDATE DBT_INTERNAL_DEST
  SET edp_expiry_dttm = dateadd(ms, -1, DBT_INTERNAL_SOURCE.edp_expiry_dttm),
  edp_last_modified_dttm = DBT_INTERNAL_SOURCE.edp_expiry_dttm,
  iud_flag = DBT_INTERNAL_SOURCE.iud_flag,
  is_current_flag = 'N',
  dbt_process_id = DBT_INTERNAL_SOURCE.dbt_process_id
  FROM {{ target }} as DBT_INTERNAL_DEST
  INNER JOIN {{ source }} as DBT_INTERNAL_SOURCE
  on DBT_INTERNAL_SOURCE.scd_id = {{ strategy.dest_scd_id}}
  WHERE DBT_INTERNAL_DEST.edp_expiry_dttm = cast('9999-12-31T23:59:59.999Z' as datetime2(6))
  AND DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete');

  INSERT INTO {{ target }} ({{ insert_cols_csv }})
  SELECT {{target_columns}} FROM {{ source }} as DBT_INTERNAL_SOURCE
  WHERE  DBT_INTERNAL_SOURCE.dbt_change_type = 'insert';

{% endmacro %}

-- -- customised solution for dms data source with append-only
-- {% macro edp_snapshot_merge_sql_dms(target, source, insert_cols, strategy) -%}
--     {%- set insert_cols_csv = insert_cols | join(', ') -%}

--     merge into {{ target }} as DBT_INTERNAL_DEST
--     using {{ source }} as DBT_INTERNAL_SOURCE
--     on DBT_INTERNAL_SOURCE.scd_id = {{ strategy.dest_scd_id}}
--     when matched
--      and DBT_INTERNAL_DEST.audit_to_timestamp = date_format('9999-12-31T23:59:59.999Z', 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'')::timestamp
--      and DBT_INTERNAL_SOURCE.dbt_change_type in ('update')
--         then update
--         set audit_to_timestamp = date_format(DBT_INTERNAL_SOURCE.audit_to_timestamp - INTERVAL 1 milliseconds, 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'')::timestamp, audit_active_ind = 'N',
--         audit_last_modified_timestamp = DBT_INTERNAL_SOURCE.audit_last_modified_timestamp
--     when not matched
--      and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
--         then insert ({{ insert_cols_csv }})
--         values ({{ insert_cols_csv }})

-- {% endmacro %}

{% macro edp_snapshot_merge_sql_append_only(target, source, insert_cols, strategy) -%}

  {%- set insert_cols_csv = insert_cols | join(', ') -%}

  {% set target_columns_list = [] %}

  {% for column in insert_cols %}
    {% set target_columns_list = target_columns_list.append("DBT_INTERNAL_SOURCE."+column)  %}
  {% endfor %}

  {%- set target_columns = target_columns_list | join(', ') -%}

  UPDATE DBT_INTERNAL_DEST
  SET edp_expiry_dttm = dateadd(ms, -1, DBT_INTERNAL_SOURCE.edp_expiry_dttm),
  edp_last_modified_dttm = DBT_INTERNAL_SOURCE.edp_expiry_dttm,
  iud_flag = DBT_INTERNAL_SOURCE.iud_flag,
  is_current_flag = 'N',
  dbt_process_id = DBT_INTERNAL_SOURCE.dbt_process_id
  FROM {{ target }} as DBT_INTERNAL_DEST
  INNER JOIN {{ source }} as DBT_INTERNAL_SOURCE
  on DBT_INTERNAL_SOURCE.scd_id = {{ strategy.dest_scd_id}}
  WHERE DBT_INTERNAL_DEST.edp_expiry_dttm = cast('9999-12-31T23:59:59.999Z' as datetime2(6))
  AND DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete');

  INSERT INTO {{ target }} ({{ insert_cols_csv }})
  SELECT {{target_columns}} FROM {{ source }} as DBT_INTERNAL_SOURCE
  WHERE  DBT_INTERNAL_SOURCE.dbt_change_type = 'insert';

{% endmacro %}

{% macro edp_snapshot_merge_sql(target, source, insert_cols, strategy) -%}
    {%- set insert_cols_csv = insert_cols | join(', ') -%}

    merge into {{ target }} as DBT_INTERNAL_DEST
    using {{ source }} as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.scd_id = {{ strategy.dest_scd_id}}

    when matched
     and DBT_INTERNAL_DEST.audit_to_timestamp = date_format('9999-12-31T23:59:59.999Z', 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'')::timestamp
     and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
        then update
        set audit_to_timestamp = date_format(DBT_INTERNAL_SOURCE.audit_to_timestamp - INTERVAL 1 milliseconds, 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'')::timestamp, audit_active_ind = 'N',
            audit_last_modified_timestamp = DBT_INTERNAL_SOURCE.audit_last_modified_timestamp
    when not matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
        then insert ({{ insert_cols_csv }})
        values ({{ insert_cols_csv }})


{% endmacro %}