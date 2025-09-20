WITH final 
as(
    SELECT DISTINCT 

    property_type_id AS original_source_id,  
    property_type as "type"



    FROM {{ref('sales_transformed')}}

    WHERE
     property_type_id IS NOT NULL
     AND
      property_type IS NOT NULL

)

SELECT {{dbt_utils.generate_surrogate_key(['original_source_id'])}} as property_type_id,*  --hashed id for keys format consisitency between dim table 
FROM final
