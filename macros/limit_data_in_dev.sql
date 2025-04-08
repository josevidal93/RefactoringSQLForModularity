{% macro limit_data_in_dev(column_name, past_days = 1,init_date = current_timestamp) -%}

    {% if target.name == 'dev' %}
        where {{column_name}} >= dateadd( 'day', -{{past_days}}, '{{init_date}}')
    {% endif %}

{%- endmacro %}