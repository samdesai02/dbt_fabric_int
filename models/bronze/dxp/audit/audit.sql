
{{
    config(
        materialized="edp_snapshot",
        unique_key="id",
        strategy="edp_bronze_full_history",
        check_cols="all",
        source_stream="append_only",
        source_name = "edp_gtm_dev_lh",
        source_table="stg_dxp_audit",
        invalidate_hard_deletes=true
        )
}}
 
{% set source_name =  "edp_gtm_dev_lh" %}    
{% set source_table = "stg_dxp_audit" %}
 
{{
    generate_bronze_B3_model(
        source_name = source_name,
        table = source_table
    )
}}
