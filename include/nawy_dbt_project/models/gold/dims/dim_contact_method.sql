WITH final 
as(
    SELECT DISTINCT 

    method_of_contact AS contact_method



    FROM {{ref('leads_transformed')}}

    WHERE
     method_of_contact IS NOT NULL
    
)

SELECT {{dbt_utils.generate_surrogate_key(['contact_method'])}} as contact_method_id,*  --hashed id for keys format consisitency between dim tables 
FROM final
