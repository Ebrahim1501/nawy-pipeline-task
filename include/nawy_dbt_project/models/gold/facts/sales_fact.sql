WITH enrichment AS

(
    SELECT *,unit_value-actual_value AS discounted_amount,
             (contraction_date::DATE - reservation_last_update_date::DATE) AS days_to_contraction 
             FROM {{ref('sales_transformed')}}
    
)


SELECT DISTINCT
s.sale_id as id,
l.lead_id,
ltyp.lead_type_id as lead_type_id,
camp.campaign_id AS lead_campaign_id,
loc.location_id,
ptyp.property_type_id,
s.unit_value,
s.expected_value,
s.actual_value,
s.discounted_amount,
resdat.date_id AS updated_reservation_date_id,
contrdat.date_id AS contraction_date_id,
s.days_to_contraction,
s.years_of_payment ,
cat.category_id as sale_category_id,
s.is_finalized



FROM

enrichment s

JOIN {{ref('leads_transformed')}} lead ON {{dbt_utils.generate_surrogate_key(['s.lead_original_source_id'])}} = lead.lead_id AND  s.is_orphan_sale IS FALSE

LEFT JOIN {{ref('dim_location')}} loc  ON {{dbt_utils.generate_surrogate_key(['s.area_id','s.compound_id'])}} = loc.location_id

LEFT JOIN {{ref('dim_sale_category')}} cat  ON {{dbt_utils.generate_surrogate_key(['sale_category'])}} = cat.category_id

LEFT JOIN {{ref('dim_lead_scd2')}} l  ON {{dbt_utils.generate_surrogate_key(['s.lead_original_source_id'])}} = l.lead_id AND l.is_active IS TRUE

LEFT JOIN {{ref('dim_lead_type')}} ltyp  ON {{dbt_utils.generate_surrogate_key(['lead.lead_type_id'])}} = ltyp.lead_type_id 

LEFT JOIN {{ref('dim_market_campaign')}} camp  ON {{dbt_utils.generate_surrogate_key(['lead.campaign'])}} = camp.campaign_id 

LEFT JOIN {{ref('dim_property_type')}} ptyp  ON {{dbt_utils.generate_surrogate_key(['s.property_type_id'])}} = ptyp.property_type_id 

LEFT JOIN {{ref('dim_date')}} resdat  ON {{dbt_utils.generate_surrogate_key(['GREATEST(s.reservation_date::DATE,s.reservation_last_update_date::DATE)'])}} = resdat.date_id 

LEFT JOIN {{ref('dim_date')}} contrdat  ON {{dbt_utils.generate_surrogate_key(['s.contraction_date::DATE'])}} = contrdat.date_id 
