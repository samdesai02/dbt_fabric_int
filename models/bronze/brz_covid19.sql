{{
    config(
        materialized="edp_snapshot",
        unique_key="id",
        strategy="edp_bronze_full_history",
        check_cols="all",
        source_stream="append_only",
        source_name = "covid_data",
        source_table="raw_covid_data",
        invalidate_hard_deletes=true
        )
}}
 
{% set source_name =  "covid_data" %}    
{% set source_table = "raw_covid_data" %}
 
{{
    generate_bronze_B3_model(
        source_name = source_name,
        table = source_table
    )
}}
