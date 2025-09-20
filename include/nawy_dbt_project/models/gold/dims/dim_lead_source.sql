WITH final 
as(
    SELECT DISTINCT 

    lead_source 



    FROM {{ref('leads_transformed')}}

    WHERE
     lead_source IS NOT NULL
    
)

SELECT {{dbt_utils.generate_surrogate_key(['lead_source'])}} as lead_source_id,*  --hashed id for keys format consisitency between dim tables 
FROM final
