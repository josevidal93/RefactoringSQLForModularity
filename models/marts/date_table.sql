{% set start_date = "'2020-01-01'" %}
{% set end_date = "dateadd(day, 1, current_date)" %}

{{ date_spine(
    datepart="day",
    start_date=start_date,
    end_date=end_date
) }}
