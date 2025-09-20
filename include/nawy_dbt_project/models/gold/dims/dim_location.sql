WITH all_data 
as(
    SELECT

     DISTINCT 
    area_id ,  
    compound_id,
    unit_location as "location"



    FROM {{ref('sales_transformed')}}

    WHERE
     area_id IS NOT NULL
      OR
      compound_id IS NOT NULL 
    
    UNION
    SELECT

     DISTINCT 
    area_id ,  
    compound_id,
    NULL AS "location" 



    FROM {{ref('leads_transformed')}}

    WHERE
     area_id IS NOT NULL
      OR
      compound_id IS NOT NULL 
    

),
final as
(
    SELECT *,ROW_NUMBER() OVER(PARTITION BY area_id,compound_id ORDER BY "location" DESC NULLS LAST) as rn
    FROM all_data



)


SELECT {{dbt_utils.generate_surrogate_key(['area_id','compound_id'])}} as location_id,  --hashed id for keys format consisitency between dim tables 
area_id,
compound_id,
"location"

FROM final 
where rn=1
