{# This is the macro for generating the yaml file for the model #}
{#
Steps:
    1. update the list in the model_names variable
    2. Click the compile button at the bottom
    3. Copy and paste the result to the new yaml file
#}
{% set model_name_list = ["fct_dxp_visit_summary"] %}

{{ codegen.generate_model_yaml(model_names=model_name_list) }}
