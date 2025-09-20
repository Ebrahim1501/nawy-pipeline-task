
    
    WITH final AS
    (
    SELECT DISTINCT 
    sale_category AS category 
     
    FROM {{ref('sales_transformed')}} 
    WHERE sale_category IS NOT NULL 
    )
    SELECT {{dbt_utils.generate_surrogate_key(['category'])}} as category_id,* 
FROM final
