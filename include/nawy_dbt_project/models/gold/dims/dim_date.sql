with all_date as
(
SELECT
 DISTINCT {{extract_date_parts('reservation_date')}}     --fulldate,year,month,day,quarter....
FROM
{{ref('sales_transformed')}} 

WHERE reservation_date is not null

UNION 


SELECT

 DISTINCT {{extract_date_parts('contraction_date')}} 

FROM

{{ref('sales_transformed')}} 

WHERE contraction_date is not null

UNION

SELECT
 DISTINCT {{extract_date_parts('reservation_last_update_date')}} 

FROM
{{ref('sales_transformed')}} 

WHERE reservation_last_update_date is not null

UNION 

SELECT
 DISTINCT {{extract_date_parts('created_at')}}     
FROM
{{ref('leads_transformed')}} 


UNION 

SELECT
 DISTINCT {{extract_date_parts('updated_at')}}     
FROM
{{ref('leads_transformed')}} 


)



SELECT {{   dbt_utils.generate_surrogate_key(['full_date']) }} AS date_id,*
FROM all_date