{% macro standardize_text(location_col, words_to_remove=[]) %}
      
    {% set expr = "lower(" ~ location_col ~ ")" %}

    {% for w in words_to_remove %}
        {% set expr = "replace(" ~ expr ~ ", '" ~ w | lower ~ "', '')" %} 
    {% endfor %}
      
    nullif(
        nullif(
        trim(
            regexp_replace(
                regexp_replace(
                    {{ expr }},
                    '[^a-z0-9 ]',  
                    ' ',
                    'g'
                ),
                '\s+',          
                ' ',
                'g'
            )
        ),
        'none'
    ),'')
{% endmacro %}  