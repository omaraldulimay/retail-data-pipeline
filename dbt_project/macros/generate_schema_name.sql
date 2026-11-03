{# Use the folder schema as is (staging, quality, marts) instead of prefixing it with "main_". #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name is not none else target.schema }}
{%- endmacro %}
