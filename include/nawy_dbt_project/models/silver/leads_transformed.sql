{{config(materialized='table')}}

WITH cleaned_data AS
(
    SELECT

    DISTINCT
    
    id :: bigint  as original_source_id,
    
    COALESCE(buyer,FALSE)::boolean AS is_buyer, --default to false
    
    COALESCE(seller,FALSE)::boolean as is_seller, 
    
    best_time_to_call as best_time_to_call,
    
    budget::NUMERIC as budget,
    
    created_at::TIMESTAMP as created_at,
    
    updated_at::TIMESTAMP as updated_at,
    
    user_id::bigint as user_id,
    
    LOWER(TRIM(location )) as prefered_location,
    
    date_of_last_contact::TIMESTAMP as last_contact_date,
    
    TRIM(LOWER(status_name)) as status_name, 
    
    COALESCE(commercial::boolean,FALSE) as is_commercial,
    
    COALESCE(merged::BOOLEAN,FALSE) as is_merged,
    
    area_id::BIGINT as area_id,
    
    compound_id::BIGINT as compound_id,
    
    developer_id::BIGINT as developer_id,

    CASE WHEN meeting_flag>= 1 then 1 else 0 END ::BOOLEAN as meeting_flag, 
   
    do_not_call::BOOLEAN as do_not_call,
    
    lead_type_id::INT as lead_type_id,
    
    customer_id ::INT as customer_id,
    
{{ standardize_text('method_of_contact') }} AS method_of_contact,
    
    {{ standardize_text('lead_source') }} as lead_source,
    
    NULLIF(TRIM(LOWER(campaign)),'(none)') AS campaign, --for future transformations in dim tables
    
    {{ standardize_text('lead_type') }} as lead_type ,
    
    loaded_at


FROM {{ref('leads_stg')}}

),
dedupe AS
(
    SELECT *,ROW_NUMBER() OVER(PARTITION by original_source_id ORDER BY updated_at DESC) as rn FROM cleaned_data 



)




SELECT  {{ dbt_utils.generate_surrogate_key(['original_source_id']) }} AS lead_id ,*  --preserve the original lead source id as it ensure deduplication amongst all rows

FROM dedupe 
where rn = 1



