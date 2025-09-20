{% macro remove_words(column_name, words_to_remove=[]) %}
    
    {% set expr = column_name %}

    {% for w in words_to_remove %}
        {% set expr = "regexp_replace(" ~ expr ~ ", '(?i)(^|\\s)" ~ w ~ "($|\\s)', ' ', 'g')" %}
    {% endfor %}

    
{% endmacro %}
