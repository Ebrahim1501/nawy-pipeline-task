-- WITH enrichment AS

-- (
--     SELECT *,expected_value-actual_value AS discounted_amount,
--              (contraction_date::DATE - reservation_last_update_date::DATE) AS days_to_contraction 
--              FROM {{ref('sales_transformed')}}
    
-- )


SELECT DISTINCT ON(l.lead_id)
l.lead_id as id,
scd2_l.lead_scd_id, --key of the scd2 dim lead table
l.user_id,
l.customer_id,
--l.area_id as lead_area_id,
--l.developer_id as lead_developer_id,
--l.compound_id as lead_compound_id,
loc.location_id AS lead_location_id,
ltype.lead_type_id as lead_type_id,
sources.lead_source_id,
camp.campaign_id as campaign_id,
methods.contact_method_id,
COALESCE(s.is_finalized,False) as is_a_deal,
d1.date_id as created_at_date_id ,
d2.date_id as updated_at_date_id


FROM

 {{ref('leads_transformed')}} l 

LEFT JOIN {{ref('sales_transformed')}} s ON {{dbt_utils.generate_surrogate_key(['s.lead_source_id'])}} = l.lead_id 

LEFT JOIN {{ref('dim_lead_scd2')}} scd2_l  ON l.lead_id = scd2_l.lead_id AND scd2_l.is_active IS TRUE

LEFT JOIN {{ref('dim_lead_type')}} ltype  ON {{dbt_utils.generate_surrogate_key(['l.lead_type_id'])}} = ltype.lead_type_id 

LEFT JOIN {{ref('dim_market_campaign')}} camp  ON {{dbt_utils.generate_surrogate_key(['l.campaign'])}} = camp.campaign_id 

LEFT JOIN {{ref('dim_contact_method')}} methods  ON {{dbt_utils.generate_surrogate_key(['l.method_of_contact'])}} = methods.contact_method_id 

LEFT JOIN {{ref('dim_location')}} loc  ON {{dbt_utils.generate_surrogate_key(['l.area_id','l.compound_id'])}} = loc.location_id 


LEFT JOIN {{ref('dim_lead_source')}} sources  ON {{dbt_utils.generate_surrogate_key(['l.lead_source'])}} = sources.lead_source_id 

LEFT JOIN {{ref('dim_date')}} d1  ON {{dbt_utils.generate_surrogate_key(['l.created_at::DATE'])}} = d1.date_id 

LEFT JOIN {{ref('dim_date')}} d2  ON {{dbt_utils.generate_surrogate_key(['l.updated_at::DATE'])}} = d2.date_id 


