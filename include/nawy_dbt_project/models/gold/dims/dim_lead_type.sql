
    
    WITH final AS
    (
    SELECT DISTINCT 
    lead_type_id as original_source_id,
    lead_type as "type" 
    FROM {{ref('leads_transformed')}} 
    WHERE lead_type IS NOT NULL AND lead_type_id IS NOT NULL 
    )
    SELECT {{dbt_utils.generate_surrogate_key(['original_source_id'])}} as lead_type_id,* 
FROM final
