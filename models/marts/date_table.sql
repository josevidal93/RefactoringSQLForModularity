

{% set start_date = '01/01/2020' %}
{% set end_date   = current_timestamp %}


 {{  date_spine(
    "day",
    "to_date('01/01/2025', 'mm/dd/yyyy')",
    "dateadd('week', 1, current_date)"
)  }} 


