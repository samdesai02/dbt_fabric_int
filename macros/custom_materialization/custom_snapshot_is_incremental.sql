-- macro to determine if we want to rebuild the edp_snapshot model or only run
-- the incremental load
{% macro snapshot_is_incremental() %}
    --  determines whether code is executing or being just parsed for compilation or documentation
    {% if not execute %}
        {{ return(False) }}
    {% else %}
    -- get the database, schema and object type of current model
        {% set relation = adapter.get_relation(this.database, this.schema, this.table) %}
        -- returns true if the onject exists and object is table and materialization is edp_snapshot and not object maked for full refresh
        {{ return(relation is not none
                  and relation.type == 'table'
                  and model.config.materialized == 'edp_snapshot'
                  and not should_full_refresh()) }}
    {% endif %}
{% endmacro %}