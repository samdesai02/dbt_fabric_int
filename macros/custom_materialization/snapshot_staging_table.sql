-- This is customised for databricks append-only use case
-- Since the dbt original snapshot could only handle the situation where the following conditions could be met
-- 1. There is one row per unique id
-- 2. In the source data, the original row gets overwritten when a record is modified (and not appended).
{% macro edp_snapshot_staging_table_append_only(strategy, source_sql, target_relation) -%}
    -- CTE to get the source raw table we want to snapshot
    with snapshot_query as (
        {{ source_sql }}
    ),

    drop_duplicates as (
        select distinct *,
        {{ strategy.hash_string }} as row_hash,
        {{ strategy.pk_hash_string}} as pk_hash
        from snapshot_query
    ),

    -- CTE to get the current snapshotted (target) table with SCD 2 history stitching and 
    -- only take the active records 
    -- so wouldn't be any duplicates with the idential primary/unique key
    snapshotted_data as (
        select *,
            {{ strategy.unique_key }} as dbt_unique_key
        from {{ target_relation }}
        where is_current_flag='Y'
    ),

    -- CTE to add some audit columns in snapshot_query as the source to construct the 'insert' data 
    -- for the merge statement
    insertions_source_data as (
        select
            *,
            row_number() over (partition by {{ strategy.unique_key }} order by {{ strategy.updated_at }}) as row_num,
            {{ strategy.unique_key }} as dbt_unique_key, 
            {{ strategy.updated_at }} as dbt_updated_at,          
            '{{ strategy.session_id }}' as audit_process_id,           
            {{ strategy.scd_id }} as scd_id
        from drop_duplicates
    ),

    -- CTE to add some audit columns in snapshot_query as the source to construct the 'update' data 
    -- for the merge statement
    updates_source_data as (

        select
            *,
            row_number() over (partition by {{ strategy.unique_key }} order by {{ strategy.updated_at }}) as row_num,
            {{ strategy.unique_key }} as dbt_unique_key, 
            {{ strategy.updated_at }} as dbt_updated_at,     
            '{{ strategy.session_id }}' as audit_process_id 
        from drop_duplicates

    ),  

    -- CTE to join insertions_source_data with snapshotted_data to pick up all recrods that needed to be 
    -- inserted into the target table (including both the new records with non-existing unique key or 
    -- any modified records to the existing ones in the target table)
    -- this step also handles the case where duplicates with the same unique ids occur in the latest refreshed data 
    -- using the window function 'lead' to generate the column 'audit_to_timestamp' 
    -- stitch the history for the duplicates
    insertions_valid_to_with_null as (
        select 
            'insert' as dbt_change_type,
            source_data.*,
            cast(SWITCHOFFSET(source_data.dbt_updated_at, 9.5*60) as datetime2(6)) as edp_effective_dttm,
            lead(source_data.dbt_updated_at, 1)
            over (partition by source_data.dbt_unique_key order by source_data.dbt_updated_at) as edp_expiry_dttm
        from insertions_source_data source_data
        left outer join snapshotted_data
        on source_data.dbt_unique_key = snapshotted_data.dbt_unique_key
        where snapshotted_data.dbt_unique_key is null
             or (snapshotted_data.dbt_unique_key is not null
                and {{ strategy.row_changed }})
    ),

    -- Note that using the window function 'lead' will cause the most recent record of the duplicates 
    -- with the same unique having the 'effective_to_timestamp_with_null' equal to 'null' 
    -- but we intend to make it be '9999-12-31 23:59:59.999' so this step creates CTE with 
    -- the correct 'effective_to_timestamp' and 'active_ind' columns with the desired values
    insertions as (
        select dbt_change_type,
        {{strategy.all_columns_name}},
        {{strategy.adf_pipeline_id}},
        scd_id,
        edp_effective_dttm,
        case
            when edp_expiry_dttm is not null 
            then dateadd(ms, -1, edp_effective_dttm)
            else cast('9999-12-31T23:59:59.999Z' as datetime2(6))               
            end as edp_expiry_dttm,
        cast(SWITCHOFFSET({{ snapshot_get_time() }}, 9.5*60) as datetime2(6)) as edp_last_modified_dttm,
        case
            when edp_expiry_dttm is not null then 'N'
            else 'Y'
            end as is_current_flag,
        row_hash,
        pk_hash,
        '{{ strategy.session_id }}' as dbt_process_id
        from insertions_valid_to_with_null
    ),

    -- CTE to join updates_source_data and snapshotted_data to get all records that need to be invalidated in the 
    -- target table
    -- we basically want to filter only the records with the same unique id in both 'updates_source_data' and 
    -- 'snapshotted_data' and also these records need to have 'dbt_updated_at' greater than 'dbt_updated_at' 
    -- of their matched rows in the target table ('snapshotted_data')
    updates_with_row_number as (
        select 
            'update' as dbt_change_type,
            source_data.*,
            {{ strategy.staging_scd_id }} as scd_id,
            cast(SWITCHOFFSET(source_data.dbt_updated_at, 9.5*60) as datetime2(6)) as edp_effective_dttm,
            cast(source_data.dbt_updated_at as datetime2(6)) as edp_expiry_dttm,
            cast(SWITCHOFFSET({{ snapshot_get_time() }}, 9.5*60) as datetime2(6)) as edp_last_modified_dttm
        from updates_source_data as source_data
        join snapshotted_data
        on source_data.dbt_unique_key = snapshotted_data.dbt_unique_key
        where snapshotted_data.dbt_unique_key is not null
        and {{ strategy.row_changed }}
        and source_data.row_num = 1
    ),

    updates as (
        select
            dbt_change_type,
            {{strategy.all_columns_name}},
            {{strategy.adf_pipeline_id}},
            scd_id,
            edp_effective_dttm,
            edp_expiry_dttm,
            edp_last_modified_dttm,
            'N' as is_current_flag,
            row_hash,
            pk_hash,
            '{{ strategy.session_id }}' as dbt_process_id
        from updates_with_row_number
    )

    -- Union 'insertions' and 'updates' to get CTE as the source of merge statement
    (select dbt_change_type,   
        {{strategy.all_columns_name}},
        {{strategy.adf_pipeline_id}},
        scd_id,
        edp_effective_dttm,
        edp_expiry_dttm,
        edp_last_modified_dttm,
        is_current_flag,
        'I' as iud_flag,
        row_hash,
        pk_hash,
        dbt_process_id
        from insertions)
    union all
    (select dbt_change_type,
        {{strategy.all_columns_name}},
        {{strategy.adf_pipeline_id}},
        scd_id,
        edp_effective_dttm,
        edp_expiry_dttm,
        edp_last_modified_dttm,
        is_current_flag,
        'U' as iud_flag,
        row_hash,
        pk_hash,
        dbt_process_id
    from updates)
{%- endmacro %}

-- This is the most up-to-date macro to process the Fleetsu api data in the delta run
-- It builds the staging table based upon which the upsertion into the snapshotted table
-- take place, this latest version is able to handle duplicates recrods with the same
-- unique key compared to the previous one below 
{% macro edp_snapshot_staging_table_full_extract(strategy, source_sql, target_relation) -%}
    {# {{log('api delta build', info=true)}} #}
    with snapshot_query as (

        {{ source_sql }}

    ),
    
    drop_duplicates as (
        select distinct *,
        {{ strategy.hash_string }} as row_hash,
        {{ strategy.pk_hash_string}} as pk_hash
        from snapshot_query
    ),

    snapshotted_data as (

        select *,
            {{ strategy.unique_key }} as dbt_unique_key
        from {{ target_relation }}
        where is_current_flag='Y'
    ),

    insertions_source_data as (
        select
            *,
            row_number() over (partition by {{ strategy.unique_key }} order by {{ strategy.updated_at }}) as row_num,
            {{ strategy.unique_key }} as dbt_unique_key,
            {{ strategy.updated_at }} as dbt_updated_at, 
            '{{ strategy.session_id }}' as audit_process_id,
            {{ strategy.scd_id }} as scd_id

        from drop_duplicates
    ),

    updates_source_data as (

        select
            *,
            row_number() over (partition by {{ strategy.unique_key }} order by {{ strategy.updated_at }}) as row_num,
            {{ strategy.unique_key }} as dbt_unique_key,
            {{ strategy.updated_at }} as dbt_updated_at,             
            '{{ strategy.session_id }}' as audit_process_id    

        from drop_duplicates    
    ),

    {%- if strategy.invalidate_hard_deletes %}

    deletes_source_data as (

        select 
            *,
            row_number() over (partition by {{ strategy.unique_key }} order by {{ strategy.updated_at }}) as row_num,
            {{ strategy.unique_key }} as dbt_unique_key,
            {{ strategy.updated_at }} as dbt_updated_at,
            '{{ strategy.session_id }}' as audit_process_id

        from drop_duplicates    
        {# from get_latest_processed_timestamp #}
    ),
    {% endif %}

    insertions_valid_to_with_null as (

        select
            'insert' as dbt_change_type,
            source_data.*,
            cast(SWITCHOFFSET(source_data.dbt_updated_at, 9.5*60) as datetime2(6)) as edp_effective_dttm,
            lead(source_data.dbt_updated_at, 1)
            over (partition by source_data.dbt_unique_key order by source_data.dbt_updated_at) as edp_expiry_dttm           
        from insertions_source_data as source_data
        left outer join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where not (
            snapshotted_data.dbt_unique_key is not null 
            and source_data.row_num = 1
            and not {{ strategy.row_changed }}
        )
    ),

    insertions as (
        select dbt_change_type,
        {{strategy.all_columns_name}},
        {{strategy.adf_pipeline_id}},
        scd_id,
        edp_effective_dttm,
            case
                when edp_expiry_dttm is not null 
                then dateadd(ms, -1, edp_expiry_dttm)
                else cast('9999-12-31T23:59:59.999Z' as DATETIME2(6))
            end as edp_expiry_dttm,
            cast(SWITCHOFFSET({{ snapshot_get_time() }}, 9.5*60) as datetime2(6)) as edp_last_modified_dttm,
            case
                when edp_expiry_dttm is not null then 'N'
                else 'Y'
            end as is_current_flag,
        dbt_unique_key,
        dbt_updated_at,
        {{ strategy.hash_string }} as row_hash,
        {{ strategy.pk_hash_string}} as pk_hash,
        '{{ strategy.session_id }}' as dbt_process_id
        from insertions_valid_to_with_null
    ),

    insertions_with_row_number as (
        select *,
        row_number() over (partition by insertions.dbt_unique_key order by insertions.dbt_updated_at) as row_number_interim
        from insertions
    ),

    insertions_top_one as (
        select dbt_change_type,
        scd_id,
        {{strategy.all_columns_name}},
        {{strategy.adf_pipeline_id}},
        edp_effective_dttm,
        edp_expiry_dttm,
        edp_last_modified_dttm,
        is_current_flag,
        dbt_unique_key
        from insertions_with_row_number
        where row_number_interim = 1
    ),

    updates_with_row_number as (

        select
            'update' as dbt_change_type,
            source_data.*,
            {{ strategy.staging_scd_id }} as scd_id,
            cast(snapshotted_data.edp_effective_dttm as datetime2(6)) as edp_effective_dttm,
            case
                when insertions_top_one.edp_effective_dttm is not null 
                then cast(insertions_top_one.edp_effective_dttm as datetime2(6))
                else cast (source_data.dbt_updated_at as datetime2(6))
            end as edp_expiry_dttm,
            cast(SWITCHOFFSET({{ snapshot_get_time() }}, 9.5*60) as datetime2(6)) as edp_last_modified_dttm,
            'N' as active_ind,
            row_number() over (partition by source_data.dbt_unique_key order by source_data.dbt_updated_at) as row_number_interim
        from updates_source_data as source_data
        join snapshotted_data 
        on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        left join insertions_top_one
        on source_data.dbt_unique_key = insertions_top_one.dbt_unique_key
        where (
            source_data.row_num = 1
            and {{ strategy.row_changed }}
        )
        or (
            source_data.row_num = 2
        )
    ),

    updates as (

        select
            dbt_change_type,
            {{strategy.all_columns_name}},
            {{strategy.adf_pipeline_id}},
            scd_id,
            edp_effective_dttm,
            edp_expiry_dttm,
            edp_last_modified_dttm,
            'N' as is_current_flag,
            row_hash,
            pk_hash,
            '{{ strategy.session_id }}' as dbt_process_id
        from updates_with_row_number
        where row_number_interim = 1
    )

    {%- if strategy.invalidate_hard_deletes -%}
    ,

    deletes as (
    
        select
            'delete' as dbt_change_type,
            source_data.*,
            {{ strategy.staging_scd_id }} as scd_id,
            cast(SWITCHOFFSET({{ snapshot_get_time() }}, 9.5*60) as datetime2(6)) as edp_effective_dttm,           
            cast(SWITCHOFFSET({{ snapshot_get_time() }}, 9.5*60) as datetime2(6)) as edp_expiry_dttm,
            cast(SWITCHOFFSET({{ snapshot_get_time() }}, 9.5*60) as datetime2(6)) as edp_last_modified_dttm,
            'N' as is_current_flag,
            '{{ strategy.session_id }}' as dbt_process_id
        from snapshotted_data
        left join deletes_source_data as source_data 
        on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where source_data.dbt_unique_key is null
    )
    {%- endif %}

    (select dbt_change_type,
    {{strategy.all_columns_name}},
    {{strategy.adf_pipeline_id}},
    scd_id,
    edp_effective_dttm,
    edp_expiry_dttm,
    edp_last_modified_dttm,
    is_current_flag,
    'I' as iud_flag,
    row_hash,
    pk_hash,
    dbt_process_id
    from insertions)
    union all
    (select dbt_change_type,
    {{strategy.all_columns_name}},
    {{strategy.adf_pipeline_id}},
    scd_id,
    edp_effective_dttm,
    edp_expiry_dttm,
    edp_last_modified_dttm,
    is_current_flag,
    'U' as iud_flag,
    row_hash,
    pk_hash,
    dbt_process_id
    from updates)
    {%- if strategy.invalidate_hard_deletes %}
    union all
    select dbt_change_type,
    {{strategy.all_columns_name}},
    {{strategy.adf_pipeline_id}},
    scd_id,
    edp_effective_dttm,
    edp_expiry_dttm,
    edp_last_modified_dttm,
    is_current_flag,
    'D' as iud_flag,
    row_hash,
    pk_hash,
    dbt_process_id
    from deletes
    {%- endif %}

{%- endmacro %}

{% macro edp_snapshot_staging_table(strategy, source_sql, target_relation) -%}
    with snapshot_query as (
        {{ source_sql }}

    ),

    snapshotted_data as (

        select *,
            {{ strategy.unique_key }} as dbt_unique_key,
            {{ strategy.target_scd_id }} as scd_id
        from {{ target_relation }}
        where audit_active_ind='Y' 

    ),

    insertions_source_data as (

        select
            * except({{ strategy.updated_at }}),
            {{ strategy.unique_key }} as dbt_unique_key, 
            {{ strategy.hash_string }} as audit_row_hash,
            {{ strategy.pk_hash_string}} as pk_hash,
            '{{ strategy.source_stream }}' as audit_src_stream, 
            '{{ strategy.session_id }}' as audit_process_id,        
            date_format({{ strategy.updated_at }}, 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'')::timestamp as audit_from_timestamp,
            date_format('9999-12-31T23:59:59.999Z', 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'')::timestamp as  audit_to_timestamp,
            date_format({{ snapshot_get_time() }}, 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'')::timestamp as audit_last_modified_timestamp,
            'Y' as audit_active_ind,
            {{ strategy.source_scd_id }} as scd_id

        from snapshot_query
    ),

    updates_source_data as (

        select
            * except({{ strategy.updated_at }}),
            {{ strategy.unique_key }} as dbt_unique_key, 
            {{ strategy.hash_string }} as audit_row_hash, 
            {{ strategy.pk_hash_string}} as pk_hash,
            '{{ strategy.source_stream }}' as audit_src_stream, 
            '{{ strategy.session_id }}' as audit_process_id,        
            date_format({{ strategy.updated_at }}, 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'')::timestamp as audit_from_timestamp,
            date_format({{ strategy.updated_at }}, 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'')::timestamp as audit_to_timestamp,
            date_format({{ snapshot_get_time() }}, 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'')::timestamp as audit_last_modified_timestamp,
            'N' as audit_active_ind

        from snapshot_query
    ),

    {%- if strategy.invalidate_hard_deletes or strategy.invalidate_soft_deletes %}

    deletes_source_data as (

        select 
            *,
            {{ strategy.unique_key }} as dbt_unique_key
        from snapshot_query
    ),
    {% endif %}

    insertions as (

        select
            'insert' as dbt_change_type,
            source_data.* {%- if strategy.invalidate_soft_deletes -%} except({{ strategy.soft_deletes_flag }}) {%- endif -%}

        from insertions_source_data as source_data
        left outer join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where 
            (
                snapshotted_data.dbt_unique_key is null
            or  
                snapshotted_data.dbt_unique_key is not null and {{ strategy.row_changed }}
            )
                {% if strategy.invalidate_soft_deletes %}
                    and {{ strategy.soft_deletes_flag }} = 'N'
                {% endif %}
    ),

    updates as (

        select
            'update' as dbt_change_type,
            source_data.* {%- if strategy.invalidate_soft_deletes -%} except({{ strategy.soft_deletes_flag }}) {%- endif -%},
            snapshotted_data.scd_id

        from updates_source_data as source_data
        join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where (
            {{ strategy.row_changed }}
        )
    )

    {%- if strategy.invalidate_hard_deletes -%}
    ,

    hard_deletes as (
    
        select
            'delete' as dbt_change_type,
            source_data.* except({{ strategy.updated_at }}),
            audit_row_hash,
            pk_hash,
            '{{ strategy.source_stream }}' as audit_src_stream, 
            '{{ strategy.session_id }}' as audit_process_id,      
            date_format({{ snapshot_get_time() }}, 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'')::timestamp as audit_from_timestamp,            
            date_format({{ snapshot_get_time() }}, 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'')::timestamp as audit_to_timestamp,
            date_format({{ snapshot_get_time() }}, 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'')::timestamp as audit_last_modified_timestamp,
            'N' as audit_active_ind,
            snapshotted_data.scd_id
    
        from snapshotted_data
        left join deletes_source_data as source_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where source_data.dbt_unique_key is null
    )
    {%- endif %}


    {%- if strategy.invalidate_soft_deletes -%}
    ,

    soft_deletes as (
    
        select
            'delete' as dbt_change_type,
            source_data.* except({{ strategy.soft_deletes_flag }}, {{ strategy.updated_at }}),
            audit_row_hash,
            pk_hash,
            '{{ strategy.source_stream }}' as audit_src_stream, 
            '{{ strategy.session_id }}' as audit_process_id,      
            date_format(source_data.{{ strategy.updated_at }}, 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'')::timestamp as audit_from_timestamp,            
            date_format(source_data.{{ strategy.updated_at }}, 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'')::timestamp as audit_to_timestamp,
            date_format({{ snapshot_get_time() }}, 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'')::timestamp as audit_last_modified_timestamp,
            'N' as audit_active_ind,
            snapshotted_data.scd_id
    
        from snapshotted_data
        left join deletes_source_data as source_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where {{ strategy.soft_deletes_flag }} = 'Y'
    )
    {%- endif %}

    select * from insertions
    union all
    select * from updates
    {%- if strategy.invalidate_hard_deletes %}
    union all
    select * from hard_deletes
    {%- endif %}
    {%- if strategy.invalidate_soft_deletes %}
    union all
    select * from soft_deletes
    {%- endif %}
{%- endmacro %}

{% macro edp_build_snapshot_staging_table(strategy, sql, target_relation, source_stream) %}
    {% set tmp_relation = make_temp_relation(target_relation) %}
    {%- if source_stream == 'append_only' -%}
        {% set select = edp_snapshot_staging_table_append_only(strategy, sql, target_relation) %}
    {% elif source_stream  == 'full_extract' %}
        -- check how many rows of delta table from the source or base table
        {% set source_delta_cnt = check_table_row_cnt(sql) %}
        {% set select = edp_snapshot_staging_table_full_extract(strategy, sql, target_relation) %}

    {%- else -%}
        {% set select = edp_snapshot_staging_table(strategy, sql, target_relation) %}

    {% endif %}

    {# {{log(select,info=true)}} #}

    {% if strategy.contract_config and strategy.contract_config.enforced %}

        {% set staging_sql_clean %}
            select * except(dbt_unique_key, dbt_change_type, scd_id,
                            audit_from_timestamp, audit_to_timestamp, audit_last_modified_timestamp, 
                            audit_row_hash, audit_process_id, audit_active_ind, audit_src_stream)
            from ({{ select }} )
        {% endset %}

        {{ get_assert_columns_equivalent(staging_sql_clean) }}
    {% endif %}

    {% call statement('build_snapshot_staging_relation') %}
        {{ create_table_as(True, tmp_relation, select) }}
    {% endcall %}

    {% do return([tmp_relation, source_delta_cnt]) %}
{% endmacro %}

{% macro edp_build_snapshot_table(strategy, sql) %}

    {% if strategy.invalidate_soft_deletes is false %}
        select 
            * except({{ strategy.updated_at }}),
            date_format({{ strategy.updated_at }}, 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'')::timestamp as audit_from_timestamp,
            date_format('9999-12-31T23:59:59.999Z', 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'')::timestamp as audit_to_timestamp,
            date_format({{ snapshot_get_time() }}, 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'')::timestamp as audit_last_modified_timestamp,
            {{ strategy.hash_string }} as audit_row_hash,
            {{ strategy.pk_hash_string}} as pk_hash,
            '{{ strategy.session_id }}' as audit_process_id,
            'Y' as audit_active_ind,
            '{{ strategy.source_stream }}' as audit_src_stream
        from (
            {{ sql }}
        ) sbq
    {% else %}
        select 
            * except({{ strategy.updated_at }}, {{ strategy.soft_deletes_flag }}),
            date_format({{ strategy.updated_at }}, 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'')::timestamp as audit_from_timestamp,
            case 
                when {{ strategy.soft_deletes_flag }} = 'Y' 
                then  date_format({{ strategy.updated_at }}, 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'')::timestamp
                else date_format('9999-12-31T23:59:59.999Z', 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'')::timestamp 
            end as audit_to_timestamp,
            date_format({{ snapshot_get_time() }}, 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'')::timestamp as audit_last_modified_timestamp,
            {{ strategy.hash_string }} as audit_row_hash,
            {{ strategy.pk_hash_string}} as pk_hash,
            '{{ strategy.session_id }}' as audit_process_id,
            case  
                when {{ strategy.soft_deletes_flag }} = 'Y' then 'N'
                else 'Y'
            end as audit_active_ind,
            '{{ strategy.source_stream }}' as audit_src_stream
        from (
            {{ sql }}
        ) sbq
    {% endif %}

{% endmacro %}

{% macro edp_build_snapshot_table_full_extract(strategy, sql) %}
    {# {{log('api full build', info=true)}} #}
    
    with source as (
        {{sql}}
    ),

    drop_duplicates as (
        select distinct *
        from source
    ),


    add_audit_fields as (
        select *,
            cast(SWITCHOFFSET({{ strategy.updated_at }}, 9.5*60) as datetime2(6)) as edp_effective_dttm,
            lead({{ strategy.updated_at }}, 1)
            over (partition by {{ strategy.unique_key }} order by {{ strategy.updated_at }}) as edp_expiry_dttm,
            row_number() 
            over (partition by {{ strategy.unique_key }} order by {{ strategy.updated_at }} desc) as row_num
        from drop_duplicates
    ),

    format_audit_fields as (
        select 
            {{strategy.all_columns_name}},
            {{strategy.adf_pipeline_id}},
            edp_effective_dttm,
            case
                when edp_expiry_dttm is not null 
                then dateadd(ms, -1, edp_effective_dttm)
                else CAST('9999-12-31T23:59:59.999Z' AS DATETIME2(6))
            end as edp_expiry_dttm,

            cast(SWITCHOFFSET({{ snapshot_get_time() }}, 9.5*60) as datetime2(6)) as edp_last_modified_dttm,

            {{ strategy.hash_string }} as row_hash,
            {{ strategy.pk_hash_string}} as pk_hash,
            '{{ strategy.session_id }}' as dbt_process_id,
            
            case
                when edp_expiry_dttm is not null then 'N'
                else 'Y'
            end as is_current_flag,

            'I' as iud_flag        
        from add_audit_fields
    )


    select * from format_audit_fields
{% endmacro %}

{% macro edp_build_snapshot_table_append_only(strategy, sql) %}
    with source as (
        {{sql}}
    ),

    drop_duplicates as (
        select distinct *
        from source
    ),

    add_audit_fields as (
        select *,
        cast(SWITCHOFFSET({{ strategy.updated_at }}, 9.5*60) as datetime2(6)) as edp_effective_dttm,
        lead({{ strategy.updated_at }}, 1)
        over (partition by {{ strategy.unique_key }} order by {{ strategy.updated_at }}) as edp_expiry_dttm,
        row_number() 
        over (partition by {{ strategy.unique_key }} order by {{ strategy.updated_at }} desc) as row_num
    from drop_duplicates
    ),

    final as (
        select 
            {{strategy.all_columns_name}},
            {{strategy.adf_pipeline_id}},
            edp_effective_dttm,
            case
                when edp_expiry_dttm is not null 
                then dateadd(ms, -1, edp_effective_dttm)
                else cast('9999-12-31T23:59:59.999Z' as datetime2(6))
            end as edp_expiry_dttm,

            cast(SWITCHOFFSET({{ snapshot_get_time() }}, 9.5*60) as datetime2(6)) as edp_last_modified_dttm,

            {{ strategy.hash_string }} as row_hash,
            {{ strategy.pk_hash_string}} as pk_hash,

            '{{ strategy.session_id }}' as dbt_process_id,
            
            case
                when edp_expiry_dttm is not null then 'N'
                else 'Y'
            end as is_current_flag,
            'I' as iud_flag           
        from add_audit_fields
    )

    select * from final

{% endmacro %}
