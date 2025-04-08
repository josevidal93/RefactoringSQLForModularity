{%- macro audit_columns()%}
     , current_timestamp as update_at
     , '{{ invocation_id}}' as dbt_run_id
     , '{{ target.name }}' as dbt_target

{% endmacro -%}