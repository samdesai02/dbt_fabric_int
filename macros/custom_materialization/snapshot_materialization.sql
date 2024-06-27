{#
Add a new materialization type called edp_snapshot
This materialization type will create scd_type 2 table in the target database
#}

{% materialization edp_snapshot, default %}
   -- Get the config parameter
  {%- set config = model['config'] -%}

  {%- set target_table = model.get('alias', model.get('name')) -%}

  {%- set strategy_name = config.get('strategy') -%}
  {%- set unique_key = config.get('unique_key') -%}
  {% set contract_config = config.get('contract') %}
  -- check if schema exists else create one
  {% if not adapter.check_schema_exists(model.database, model.schema) %}
    {% do create_schema(model.database, model.schema) %}
  {% endif %}

  -- checks if target relation exists. i.e. target table is to be first time created or it already exists
  {% set target_relation_exists, target_relation = get_or_create_relation(
          database=model.database,
          schema=model.schema,
          identifier=target_table,
          type='table') -%}
-- raise exception if the target is not the table. i.e. view
  {%- if not target_relation.is_table -%}
    {% do exceptions.relation_wrong_type(target_relation, 'table') %}
  {%- endif -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
-- takes our code and dispaches to an adapter
  {% set strategy_macro = strategy_dispatch(strategy_name) %}
  {% set strategy = strategy_macro(model, "snapshotted_data", "source_data", config, target_relation_exists) %}
-- source_stream is full_extract or append only, full_refresh_mode is true if should_full_refresh returns true or target table is view.
  {%- set source_stream = config.source_stream -%}
  {%- set full_refresh_mode = (should_full_refresh()  or target_relation.is_view) -%}
-- if target table doesnt exist or full_refresh_mode is set to false then load whole table
  {% if not target_relation_exists or full_refresh_mode %}
      {# {{log('execute the full refresh', info=true)}} #}
      --edp customized macros handling
      {% if 'edp_' in strategy_name %}
        {% if source_stream  == 'append_only' %}
          {% set build_sql = edp_build_snapshot_table_append_only(strategy, model['compiled_sql']) %}
        {% elif source_stream  == 'full_extract' %}
          {% set build_sql = edp_build_snapshot_table_full_extract(strategy, model['compiled_sql']) %}
        {% else %}
          {% set build_sql = edp_build_snapshot_table(strategy, model['compiled_sql']) %}
        {% endif %}
      {% else %}
        {% set build_sql = build_snapshot_table(strategy, model['compiled_sql']) %}
      {% endif %}
-- check if any cols is configured as contract
      {# separate the model contract check from the table building stage here to avoid the audit column check #}
      {% if contract_config and contract_config.enforced %}

        {% set build_sql_remove_audit_cols %}
          select *
          from ({{ build_sql }} )
        {% endset %}

        {{ get_assert_columns_equivalent(build_sql) }}

      {% endif %}
          {# {{log('Build_sql: '~build_sql, info=true)}} #}
    {# {% set final_sql = create_table_as(False, target_relation, build_sql) %} #}
      {% set final_sql = create_table_as(False, target_relation, build_sql) %}

  {% else %} -- when table is ran second time i.e. the table is already existing
      --only do the validation for dbt snapshot
      {% if 'edp_' in strategy_name %}
        -- edp_build_snapshot_staging_table macro compare the existing table with source table to identify update, delete, or new entry.
        {% set staging_table, source_delta_cnt = edp_build_snapshot_staging_table(strategy, sql, target_relation, source_stream) %}
      {% else %}
        {{ adapter.valid_snapshot_target(target_relation) }}
        {% set staging_table = build_snapshot_staging_table(strategy, sql, target_relation) %}
      {% endif %}

      {# Comment this column out for now as no requirement for column expansion
      -- this may no-op if the database does not require column expansion
      -- Expand the to_relation table's column types to match the schema of from_relation.
      -- e.g. Typical usage involves expanding column types (from eg. varchar(16) to varchar(32)) to support insert statements.

      {% do adapter.expand_target_column_types(from_relation=staging_table,
                                               to_relation=target_relation) %}
      {{log('I am here!', info=true)}}

      {% set missing_columns = adapter.get_missing_columns(staging_table, target_relation)
                                   | rejectattr('name', 'equalto', 'dbt_change_type')
                                   | rejectattr('name', 'equalto', 'DBT_CHANGE_TYPE')
                                   | rejectattr('name', 'equalto', 'dbt_unique_key')
                                   | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
                                   | rejectattr('name', 'equalto', 'scd_id')
                                   | rejectattr('name', 'equalto', 'SCD_ID')
                                   | rejectattr('name', 'equalto', 'dbt_updated_at')
                                   | rejectattr('name', 'equalto', 'DBT_UPDATED_AT')
                                   | list %}

        {{log('missing_columns_name: '~missing_columns, info=true)}}

      {% do create_columns(target_relation, missing_columns) %}

      {% set source_columns = adapter.get_columns_in_relation(staging_table)
                                   | rejectattr('name', 'equalto', 'dbt_change_type')
                                   | rejectattr('name', 'equalto', 'DBT_CHANGE_TYPE')
                                   | rejectattr('name', 'equalto', 'dbt_unique_key')
                                   | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
                                   | rejectattr('name', 'equalto', 'scd_id')
                                   | rejectattr('name', 'equalto', 'SCD_ID')
                                   | rejectattr('name', 'equalto', 'dbt_updated_at')
                                   | rejectattr('name', 'equalto', 'DBT_UPDATED_AT')
                                   | list %}

      {% set quoted_source_columns = [] %}
      {% for column in source_columns %}
        {% do quoted_source_columns.append(adapter.quote(column.name)) %}
      {% endfor %}

      #}
      --edp customized macros handling

      {% set source_columns = adapter.get_columns_in_relation(staging_table)
                                   | rejectattr('name', 'equalto', 'dbt_change_type')
                                   | rejectattr('name', 'equalto', 'DBT_CHANGE_TYPE')
                                   | rejectattr('name', 'equalto', 'dbt_unique_key')
                                   | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
                                   | rejectattr('name', 'equalto', 'scd_id')
                                   | rejectattr('name', 'equalto', 'SCD_ID')
                                   | rejectattr('name', 'equalto', 'dbt_updated_at')
                                   | rejectattr('name', 'equalto', 'DBT_UPDATED_AT')
                                   | list %}

      {% set quoted_source_columns = [] %}
      {% for column in source_columns %}
        {% do quoted_source_columns.append(adapter.quote(column.name)) %}
      {% endfor %}

      {% if 'edp_' in strategy_name %}
        {% if source_stream == 'append_only' %}
          {% set final_sql = edp_snapshot_merge_sql_append_only(
            target = target_relation,
            source = staging_table,
            insert_cols = quoted_source_columns,
            strategy = strategy
            )
          %}
        {% elif source_stream == 'full_extract' %}
          {% set final_sql = edp_snapshot_merge_sql_full_extract(
            target = target_relation,
            source = staging_table,
            insert_cols = quoted_source_columns,
            strategy = strategy
            )
          %}
        {% else %}
          {% set final_sql = edp_snapshot_merge_sql(
            target = target_relation,
            source = staging_table,
            insert_cols = quoted_source_columns,
            strategy = strategy
            )
          %}
        {% endif %}
      {% else %}
        {% set final_sql = snapshot_merge_sql(
          target = target_relation,
          source = staging_table,
          insert_cols = quoted_source_columns
          )
        %}
      {% endif %}

  {% endif %}


{% if source_stream == 'full_extract' and source_delta_cnt is not none and source_delta_cnt == 0 %}
  -- not doing the merge if there is no delta data from the source table
  {% call statement('main') %}
      select 1
  {% endcall %}
{% else %}
  {% call statement('main') %}
      {{ final_sql }}
  {% endcall %}
{% endif %}

  {% do persist_docs(target_relation, model) %}

  {% if not target_relation_exists %}
    {% do create_indexes(target_relation) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {% if staging_table is defined %}
      {% do post_snapshot(staging_table) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}