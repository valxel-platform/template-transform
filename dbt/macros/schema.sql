{# Every model goes in the run's build schema, never a schema of its own: the runner swaps the
   whole build schema in when every test passes. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
  {{ target.schema }}
{%- endmacro %}
