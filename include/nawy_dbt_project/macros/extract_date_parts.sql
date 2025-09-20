{% macro extract_date_parts(date_col) %}
    {{date_col}}::DATE as full_date, 
    extract(year from {{ date_col }})        as year,
    extract(month from {{ date_col }})       as month,
    extract(day from {{ date_col }})         as day,
    extract(quarter from {{ date_col }})     as year_quarter,
    case extract(dow from {{ date_col }}) 
    when 0 then 'sunday'
    when 1 then 'monday'
    when 2 then 'tuesday'
    when 3 then 'wednesday'
    when 4 then 'thursday'
    when 5 then 'friday'
    when 6 then 'saturday'
    end                                     as week_day
{% endmacro %}
