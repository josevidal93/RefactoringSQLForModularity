
{% set list = ['1','2','3']             %}

{% for item in list %}
    {% if loop.first %}
    --first item
        {{ item }}
    {% endif %}
    {% if loop.last %}   
    --This is the last item
        {{ item }}
    {% endif %}
{% endfor %}







{% set rating_categories = ["quality_rating",
                            "design_rating",
                            "usability_rating"] %}
SELECT product_id,
 {%- for col_name in rating_categories -%}
   AVG({{ col_name }}) as {{ col_name }}_average
   {%- if not loop.last  -%} 
     , 
   {% endif %}
 {% endfor %}
 FROM product_reviews
 GROUP BY 1