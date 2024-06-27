{#/*
    Customized strategy definitions
    - edp_bronze_B3
    - edp_timestamp
    - edp_hashcheck
    - edp_check
*/ #}

{% macro strategy_dispatch(name) -%}

{{log('strategy name is: '~name, info=true)}}

{% set original_name = name %}
  {% if '.' in name %}
    {% set package_name, name = name.split(".", 1) %}
  {% else %}
    {% set package_name = none %}
  {% endif %}

  {% if package_name is none %}
    {% set package_context = context %}
  {% elif package_name in context %}
    {% set package_context = context[package_name] %}
  {% else %}
    {% set error_msg %}
        Could not find package '{{package_name}}', called with '{{original_name}}'
    {% endset %}
    {{ exceptions.raise_compiler_error(error_msg | trim) }}
  {% endif %}

  {%- set search_name = 'snapshot_' ~ name ~ '_strategy' -%}

  {% if search_name not in package_context %}
    {% set error_msg %}
        The specified strategy macro '{{name}}' was not found in package '{{ package_name }}'
    {% endset %}
    {{ exceptions.raise_compiler_error(error_msg | trim) }}
  {% endif %}
  {{ return(package_context[search_name]) }}
{%- endmacro %}

{% macro snapshot_edp_timestamp_strategy(node, snapshotted_rel, current_rel, config, target_exists) %}
    {% set primary_key = config['unique_key'] %}
    {% set updated_at = config['updated_at'] %}
    {% set source_stream = config['source_stream'] %}
    {% set sessionid = invocation_id %}
    {# {% set sessionid = 'dbtcli_' ~ run_started_at %} #}
    {% set invalidate_hard_deletes = config.get('invalidate_hard_deletes', false) %}
    {% set contract_config = config.get('contract') %}


    {#/*
        The snapshot relation might not have an {{ updated_at }} value if the
        snapshot strategy is changed from `check` to `timestamp`. We
        should use a dbt-created column for the comparison in the snapshot
        table instead of assuming that the user-supplied {{ updated_at }}
        will be present in the historical data.

        See https://github.com/dbt-labs/dbt-core/issues/2350
    */ #}
    {% set row_changed_expr -%}
        ({{ snapshotted_rel }}.effective_from_timestamp < {{ current_rel }}.{{ updated_at }})
    {%- endset %}

   {% set source_scd_id_expr = snapshot_hash_arguments([primary_key, updated_at]) %}
    {% set target_scd_id_expr = snapshot_hash_arguments([primary_key, 'audit_from_timestamp']) %}
    {% set dest_scd_id_expr = snapshot_hash_arguments(['DBT_INTERNAL_DEST.' ~ primary_key, 'DBT_INTERNAL_DEST.audit_from_timestamp' ]) %}


    {% if check_cols_config == 'all' %}
        {% set column_added, check_cols = snapshot_check_all_get_existing_columns(node, target_exists) %}
    {% elif check_cols_config is iterable and (check_cols_config | length) > 0 %}
        {% set check_cols = check_cols_config %}
    {% else %}
        {% do exceptions.raise_compiler_error("Invalid value for 'check_cols': " ~ check_cols_config) %}
    {% endif %}

    {% set hash_string_expr = check_hash_arguments(check_cols) %}

    {% do return({
        "unique_key": primary_key,
        "updated_at": updated_at,
        "row_changed": row_changed_expr,
        "source_scd_id": source_scd_id_expr,
        "target_scd_id": target_scd_id_expr,
        "dest_scd_id": dest_scd_id_expr,
        "hash_string": hash_string_expr,
        "source_stream": source_stream,
        "session_id": sessionid,
        "invalidate_hard_deletes": invalidate_hard_deletes,
        "contract_config": contract_config
    }) %}
{% endmacro %}


-- Customised strategy created for the bronze table history stitching only
{% macro snapshot_edp_bronze_full_history_strategy(node, snapshotted_rel, current_rel, config, target_exists) %}
    {% set check_cols_config = config['check_cols'] %}
    {% set primary_key = config['unique_key'] %}

    {%- if primary_key is not string and primary_key is iterable and (primary_key | length) > 0 -%}
        {%- set primary_key_pk_hash = primary_key %}
        {%- set primary_key = primary_key | join("+'-'+") %}
    {% else %} 
        {%- set primary_key_pk_hash = [primary_key]%}
    {%- endif -%}
    
    {% set updated_at = config.get('updated_at', snapshot_get_time()) %}
    {% set source_stream = config['source_stream'] %}
    {% set sessionid = invocation_id %}
    {# {% set sessionid = 'dbtcli_' ~ run_started_at %} #}
    {% set data_change_type = config['change_type_col'] %}
    {% set source_name = config['source_name'] %}
    {% set source_table = config['source_table'] %}
    {% set all_columns = dbt_utils.star(from=source(source_name, source_table))%}

    {% set invalidate_hard_deletes = config.get('invalidate_hard_deletes', false) %}
    {% set adf_pipeline_id %}
        '{{var("adf_pipeline_id")}}' as "adf_pipeline_id"
    {% endset %}

    
        {% set row_changed_expr -%}
        (
            {% if source_stream == 'full_extract' %}
            -- row change expr for the full_extract source table
            ({{ snapshotted_rel }}.row_hash <> {{ current_rel }}.row_hash)
            {% else %}
            -- row change expr for the delta_extract source table
            ({{ snapshotted_rel }}.row_hash <> {{ current_rel }}.row_hash) 
            {% endif %}
        )
        {%- endset %}    

    {% set scd_id_expr = snapshot_hash_arguments([primary_key, updated_at]) %}

    {% set staging_scd_id_expr = snapshot_hash_arguments([add_table_prefix_to_scd_expr('snapshotted_data', primary_key), 'snapshotted_data.edp_effective_dttm']) %}

    {% set dest_scd_id_expr = snapshot_hash_arguments([add_table_prefix_to_scd_expr('DBT_INTERNAL_DEST', primary_key), 'DBT_INTERNAL_DEST.edp_effective_dttm' ]) %}

    {% set src_scd_id_expr = snapshot_hash_arguments([add_table_prefix_to_scd_expr('DBT_INTERNAL_SOURCE', primary_key), 'DBT_INTERNAL_SOURCE.edp_effective_dttm' ]) %}

    {% if check_cols_config == 'all' %}
        {% set check_cols = dbt_utils.star(from=source(source_name, source_table), except=['audit_ingestion_time']).split(",\n") %}
    {% elif check_cols_config is iterable and (check_cols_config | length) > 0 %}
        {% set check_cols = check_cols_config %}
    {% else %}
        {% do exceptions.raise_compiler_error("Invalid value for 'check_cols': " ~ check_cols_config) %}
    {% endif %}

    {{log("check_col: "~check_cols, info=true)}}

    {% set hash_string_expr = check_hash_arguments(check_cols) %}
    {% set pk_hash_expr = check_hash_arguments(primary_key_pk_hash) %}

    {% do return({
        "unique_key": primary_key,
        "updated_at": updated_at,
        "row_changed": row_changed_expr,
        "scd_id": scd_id_expr,
        "staging_scd_id": staging_scd_id_expr,
        "dest_scd_id": dest_scd_id_expr,
        "src_scd_id": src_scd_id_expr,
        "source_stream": source_stream,
        "session_id": sessionid,
        "data_change_type": data_change_type,
        "hash_string": hash_string_expr,
        "pk_hash_string": pk_hash_expr,
        "invalidate_hard_deletes": invalidate_hard_deletes,
        "all_columns_name": all_columns,
        "adf_pipeline_id": adf_pipeline_id
    }) %}
{% endmacro %}

-- Customised strategy created for the bronze table history stitching only
{% macro snapshot_edp_silver_full_history_strategy(node, snapshotted_rel, current_rel, config, target_exists) %}
    {% set check_cols_config = config['check_cols'] %}
    {% set primary_key = config['unique_key'] %}

    {%- if primary_key is not string and primary_key is iterable and (primary_key | length) > 0 -%}
        {%- set primary_key_pk_hash = primary_key %}
        {%- set primary_key = primary_key | join("+'-'+") %}
    {% else %} 
        {%- set primary_key_pk_hash = [primary_key]%}
    {%- endif -%}
    
    {% set updated_at = config.get('updated_at', snapshot_get_time()) %}
    {% set source_stream = config['source_stream'] %}
    {% set sessionid = invocation_id %}
    {# {% set sessionid = 'dbtcli_' ~ run_started_at %} #}
    {% set data_change_type = config['change_type_col'] %}
    {% set bronze_table_name = config['bronze_table_name'] %}
    {% set all_columns %}
        {{ config['required_columns'] | join(', ') }}
    {% endset %}

    {% set invalidate_hard_deletes = config.get('invalidate_hard_deletes', false) %}
    {% set adf_pipeline_id %}
        '{{var("adf_pipeline_id")}}' as "adf_pipeline_id"
    {% endset %}

    
        {% set row_changed_expr -%}
        (
            {% if source_stream == 'full_extract' %}
            -- row change expr for the full_extract source table
            ({{ snapshotted_rel }}.row_hash <> {{ current_rel }}.row_hash)
            {% else %}
            -- row change expr for the delta_extract source table
            ({{ snapshotted_rel }}.row_hash <> {{ current_rel }}.row_hash) 
            {% endif %}
        )
        {%- endset %}    

    {% set scd_id_expr = snapshot_hash_arguments([primary_key, updated_at]) %}

    {% set staging_scd_id_expr = snapshot_hash_arguments([add_table_prefix_to_scd_expr('snapshotted_data', primary_key), 'snapshotted_data.edp_effective_dttm']) %}

    {% set dest_scd_id_expr = snapshot_hash_arguments([add_table_prefix_to_scd_expr('DBT_INTERNAL_DEST', primary_key), 'DBT_INTERNAL_DEST.edp_effective_dttm' ]) %}

    {% set src_scd_id_expr = snapshot_hash_arguments([add_table_prefix_to_scd_expr('DBT_INTERNAL_SOURCE', primary_key), 'DBT_INTERNAL_SOURCE.edp_effective_dttm' ]) %}

    {% if check_cols_config == 'all' %}
        {% set check_cols = dbt_utils.star(from=ref(bronze_table_name), except=['audit_ingestion_time']).split(",\n") %}
    {% elif check_cols_config is iterable and (check_cols_config | length) > 0 %}
        {% set check_cols = check_cols_config %}
    {% else %}
        {% do exceptions.raise_compiler_error("Invalid value for 'check_cols': " ~ check_cols_config) %}
    {% endif %}

    {{log("check_col: "~check_cols, info=true)}}

    {% set hash_string_expr = check_hash_arguments(check_cols) %}
    {% set pk_hash_expr = check_hash_arguments(primary_key_pk_hash) %}

    {% do return({
        "unique_key": primary_key,
        "updated_at": updated_at,
        "row_changed": row_changed_expr,
        "scd_id": scd_id_expr,
        "staging_scd_id": staging_scd_id_expr,
        "dest_scd_id": dest_scd_id_expr,
        "src_scd_id": src_scd_id_expr,
        "source_stream": source_stream,
        "session_id": sessionid,
        "data_change_type": data_change_type,
        "hash_string": hash_string_expr,
        "pk_hash_string": pk_hash_expr,
        "invalidate_hard_deletes": invalidate_hard_deletes,
        "all_columns_name": all_columns,
        "adf_pipeline_id": adf_pipeline_id
    }) %}
{% endmacro %}

-- Customised strategy created for the bronze table history stitching only
{% macro snapshot_edp_gold_full_history_strategy(node, snapshotted_rel, current_rel, config, target_exists) %}
    {% set check_cols_config = config['check_cols'] %}
    {% set primary_key = config['unique_key'] %}

    {%- if primary_key is not string and primary_key is iterable and (primary_key | length) > 0 -%}
        {%- set primary_key_pk_hash = primary_key %}
        {%- set primary_key = primary_key | join("+'-'+") %}
    {% else %} 
        {%- set primary_key_pk_hash = [primary_key]%}
    {%- endif -%}
    
    {% set updated_at = config.get('updated_at', snapshot_get_time()) %}
    {% set source_stream = config['source_stream'] %}
    {% set sessionid = invocation_id %}
    {# {% set sessionid = 'dbtcli_' ~ run_started_at %} #}
    {% set data_change_type = config['change_type_col'] %}
    {% set bronze_table_name = config['bronze_table_name'] %}
    {% set all_columns %}
        {{ config['required_columns'] | join(', ') }}
    {% endset %}

    {% set invalidate_hard_deletes = config.get('invalidate_hard_deletes', false) %}
    {% set adf_pipeline_id %}
        '{{var("adf_pipeline_id")}}' as "adf_pipeline_id"
    {% endset %}

    
        {% set row_changed_expr -%}
        (
            {% if source_stream == 'full_extract' %}
            -- row change expr for the full_extract source table
            ({{ snapshotted_rel }}.row_hash <> {{ current_rel }}.row_hash)
            {% else %}
            -- row change expr for the delta_extract source table
            ({{ snapshotted_rel }}.row_hash <> {{ current_rel }}.row_hash) 
            {% endif %}
        )
        {%- endset %}    

    {% set scd_id_expr = snapshot_hash_arguments([primary_key, updated_at]) %}

    {% set staging_scd_id_expr = snapshot_hash_arguments([add_table_prefix_to_scd_expr('snapshotted_data', primary_key), 'snapshotted_data.edp_effective_dttm']) %}

    {% set dest_scd_id_expr = snapshot_hash_arguments([add_table_prefix_to_scd_expr('DBT_INTERNAL_DEST', primary_key), 'DBT_INTERNAL_DEST.edp_effective_dttm' ]) %}

    {% set src_scd_id_expr = snapshot_hash_arguments([add_table_prefix_to_scd_expr('DBT_INTERNAL_SOURCE', primary_key), 'DBT_INTERNAL_SOURCE.edp_effective_dttm' ]) %}

    {% if check_cols_config == 'all' %}
        {% set check_cols = dbt_utils.star(from=ref(bronze_table_name), except=['audit_ingestion_time']).split(",\n") %}
    {% elif check_cols_config is iterable and (check_cols_config | length) > 0 %}
        {% set check_cols = check_cols_config %}
    {% else %}
        {% do exceptions.raise_compiler_error("Invalid value for 'check_cols': " ~ check_cols_config) %}
    {% endif %}

    {{log("check_col: "~check_cols, info=true)}}

    {% set hash_string_expr = check_hash_arguments(check_cols) %}
    {% set pk_hash_expr = check_hash_arguments(primary_key_pk_hash) %}

    {% do return({
        "unique_key": primary_key,
        "updated_at": updated_at,
        "row_changed": row_changed_expr,
        "scd_id": scd_id_expr,
        "staging_scd_id": staging_scd_id_expr,
        "dest_scd_id": dest_scd_id_expr,
        "src_scd_id": src_scd_id_expr,
        "source_stream": source_stream,
        "session_id": sessionid,
        "data_change_type": data_change_type,
        "hash_string": hash_string_expr,
        "pk_hash_string": pk_hash_expr,
        "invalidate_hard_deletes": invalidate_hard_deletes,
        "all_columns_name": all_columns,
        "adf_pipeline_id": adf_pipeline_id
    }) %}
{% endmacro %}

{% macro snapshot_edp_hashcheck_strategy(node, snapshotted_rel, current_rel, config, target_exists) %}
    {% set check_cols_config = config['check_cols'] %}
    {% set primary_key = config['unique_key'] %}

    {%- if primary_key is not string and primary_key is iterable and (primary_key | length) > 0 -%}
        {%- set primary_key = primary_key | join("||'-'||") %}
    {%- endif -%}

    {% set surrogate_key = config['surrogate_key'] %}
    {% set source_stream = config['source_stream'] %}
    {% set sessionid = invocation_id %}
    {% set invalidate_hard_deletes = config.get('invalidate_hard_deletes', false) %}
    {% set invalidate_soft_deletes = config.get('invalidate_soft_deletes', false) %}

     {% set soft_deletes_flag = config.get('soft_deletes_flag', 'no_soft_deletes') %}

    {% if invalidate_soft_deletes and soft_deletes_flag == 'no_soft_deletes' %}
        {{ exceptions.raise_compiler_error("soft delete is enabled on the model but the soft delete flag/column is not specified in the model config") }}
    {% endif %}

    {% set contract_config = config.get('contract') %}

    {% set select_current_time -%}
        select {{ snapshot_get_time() }} as snapshot_start
    {%- endset %}

    {#-- don't access the column by name, to avoid dealing with casing issues on snowflake #}
    {%- set now = run_query(select_current_time)[0][0] -%}
    {% if now is none or now is undefined -%}
        {%- do exceptions.raise_compiler_error('Could not get a snapshot start time from the database') -%}
    {%- endif %}
    {% set updated_at = config.get('updated_at', snapshot_string_as_time(now)) %}

    {% set row_changed_expr -%}
        ({{ snapshotted_rel }}.audit_row_hash <> {{ current_rel }}.audit_row_hash)
    {%- endset %}

    {% set source_scd_id_expr = snapshot_hash_arguments([primary_key, updated_at]) %}
    {% set target_scd_id_expr = snapshot_hash_arguments([primary_key, 'audit_from_timestamp']) %}
    {% set dest_scd_id_expr = snapshot_hash_arguments([lakehouse_utils.add_table_prefix_to_scd_expr('DBT_INTERNAL_DEST', primary_key), 'DBT_INTERNAL_DEST.audit_from_timestamp' ]) %}

    {% if check_cols_config == 'all' %}
        {% set check_cols = lakehouse_utils.get_model_cols_from_model_yml(node, surrogate_key) %}
        {% set col_names = check_cols %}
        { log("Column Names " ~ col_names, info=true)}}
        {# {% set column_added, check_cols = snapshot_check_all_get_existing_columns(node, target_exists) %} #}
    {% elif check_cols_config is iterable and (check_cols_config | length) > 0 %}
        {% set check_cols = check_cols_config %}
        {% set col_names = check_cols %}
        { log("Column Names " ~ col_names, info=true)}}
    {% else %}
        {% do exceptions.raise_compiler_error("Invalid value for 'check_cols': " ~ check_cols_config) %}
    {% endif %}

    {% set hash_string_expr = lakehouse_utils.check_hash_arguments(check_cols) %}

    {% do return({
        "unique_key": primary_key,
        "updated_at": updated_at,
        "row_changed": row_changed_expr,
        "source_scd_id": source_scd_id_expr,
        "target_scd_id": target_scd_id_expr,
        "dest_scd_id": dest_scd_id_expr,
        "hash_string": hash_string_expr,
        "source_stream": source_stream,
        "session_id": sessionid,
        "invalidate_hard_deletes": invalidate_hard_deletes,
        "invalidate_soft_deletes": invalidate_soft_deletes,
        "contract_config": contract_config,
        "soft_deletes_flag": soft_deletes_flag
    }) %}
{% endmacro %}

{% macro check_hash_arguments(args) -%}
    {% if args is iterable and (args | length) > 1 %}
        {% set hash %}
        HashBytes('MD5', concat({%- for arg in args -%}
            coalesce(cast({{ arg }} as varchar(8000) ), ''){% if not loop.last %}, {% endif %}
        {%- endfor -%}))
        {% endset %}
        {% do return(hash) %}
    {% elif args is iterable and (args | length) == 1 %}
        {% set hash %}
        HashBytes('MD5', {%- for arg in args -%}
            coalesce(cast({{ arg }} as varchar(8000)), '')
        {%- endfor -%})
        {% endset %}
        {% do return(hash) %}
    {% endif %}
{%- endmacro %}

{% macro snapshot_edp_columncheck_strategy(node, snapshotted_rel, current_rel, config, target_exists) %}
    {% set check_cols_config = config['check_cols'] %}
    {% set primary_key = config['unique_key'] %}
    {% set source_stream = var('source_stream', '-') %}
    {% set sessionid = var('sessionid', invocation_id) %}
    {% set invalidate_hard_deletes = config.get('invalidate_hard_deletes', false) %}
    {% set duplication_check = config.get('duplication_check', true) %}
    {% set contract_config = config.get('contract') %}

    {% set select_current_time -%}
        select {{ snapshot_get_time() }} as snapshot_start
    {%- endset %}

    {#-- don't access the column by name, to avoid dealing with casing issues on snowflake #}
    {%- set now = run_query(select_current_time)[0][0] -%}
    {% if now is none or now is undefined -%}
        {%- do exceptions.raise_compiler_error('Could not get a snapshot start time from the database') -%}
    {%- endif %}
    {% set updated_at = config.get('updated_at', snapshot_string_as_time(now)) %}

    {%- if duplication_check %}
        {#-- do the duplication check #}
        {%- set duplication_check_query -%}
            select {{ primary_key }}, count(*) as cnt from ({{ node.compiled_sql }})
                group by {{ primary_key }} having count(*)>1
        {%- endset -%}    
        {#-- log("duplication_check_query = " ~ duplication_check_query ) #}
        {% set duplicated_rows = run_query(duplication_check_query) %}
        {% if execute %}
            {% set duplicated_list = duplicated_rows.columns[0].values() %}
        {% else %}
            {% set duplicated_list = [] %}
        {% endif %}    
        {{ log("duplication check result: " ~ duplicated_rows[0] ~ (duplicated_rows|length)) }}
        {%- if duplicated_rows|length > 0 -%}
            {{ log("Duplication record " ~ primary_key ~ ": " ~ duplicated_list )}}
            {%- do exceptions.raise_compiler_error('User-defined Error: The source data has duplication, please check log for the details.') -%}
        {%- endif -%}
    {%- endif -%}   
    
    {% set column_added = false %}

    {% if check_cols_config == 'all' %}
        {% set column_added, check_cols = snapshot_check_all_get_existing_columns(node, target_exists) %}
        {% set col_names = check_cols %}
        { log("Column Names " ~ col_names, info=true)}}
    {% elif check_cols_config is iterable and (check_cols_config | length) > 0 %}
        {% set check_cols = check_cols_config %}
        {% set col_names = check_cols %}
        { log("Column Names " ~ col_names, info=true)}}
    {% else %}
        {% do exceptions.raise_compiler_error("Invalid value for 'check_cols': " ~ check_cols_config) %}
    {% endif %}

    {%- set row_changed_expr -%}
    (
    {%- if column_added -%}
        TRUE
    {%- else -%}
    {%- for col in check_cols -%}
        {{ snapshotted_rel }}.{{ col }} != {{ current_rel }}.{{ col }}
        or
        (
            (({{ snapshotted_rel }}.{{ col }} is null) and not ({{ current_rel }}.{{ col }} is null))
            or
            ((not {{ snapshotted_rel }}.{{ col }} is null) and ({{ current_rel }}.{{ col }} is null))
        )
        {%- if not loop.last %} or {% endif -%}
    {%- endfor -%}
    {%- endif -%}
    )
    {%- endset %}

    {% set scd_id_expr = snapshot_hash_arguments([primary_key, updated_at]) %}

    {% set hash_string_expr = check_hash_arguments(check_cols) %}

    {% set hash_string_expr = check_hash_arguments(check_cols) %}

    {% do return({ 
        "unique_key": primary_key,
        "updated_at": updated_at,
        "row_changed": row_changed_expr,
        "scd_id": scd_id_expr,
        "hash_string": hash_string_expr,
        "hash_string": hash_string_expr,
        "source_stream": source_stream,
        "session_id": sessionid,
        "invalidate_hard_deletes": invalidate_hard_deletes,
        "contract_config": contract_config
    }) %}
{% endmacro %}
