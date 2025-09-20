{{
     config(materialized='table',
    post_hook=[
        'CREATE INDEX IF NOT EXISTS lead_idx ON {{this}}(lead_source_id)',
])

}}


with cleaned_data as 
(
    SELECT 
    id ::bigint AS original_source_id,
    {{ standardize_text('sale_category')  }} as sale_category,
    lead_id::Bigint AS lead_source_id,
    area_id::bigint as area_id,
    compound_id::bigint as compound_id,
TRIM({{ standardize_text("replace(replace(replace(lower(unit_location), 'city', ''), 'of', ''), 'orascom', '')") }}) AS unit_location,
    property_type_id::int as property_type_id,
    {{  standardize_text('property_type')   }} as property_type, 
    unit_value::NUMERIC as unit_value,
    expected_value::NUMERIC as expected_value,
    CASE 
    WHEN  date_of_contraction IS NOT NULL THEN COALESCE(actual_value::NUMERIC,unit_value::NUMERIC) ELSE actual_value::NUMERIC END as actual_value,
    date_of_reservation::TIMESTAMP as reservation_date,
    COALESCE(reservation_update_date::TIMESTAMP,date_of_contraction::TIMESTAMP,date_of_reservation::TIMESTAMP) as reservation_last_update_date,
    date_of_contraction::TIMESTAMP as contraction_date,
    years_of_payment::INT as years_of_payment,

    (date_of_contraction IS NOT NULL) AS is_finalized,--boolean flag for successfully closed deals
    (lead_id IS NULL) AS is_orphan_sale,--flag for sales records without a lead entry
    loaded_at

    from {{ref('sales_stg')}}

    -- WHERE 
    -- (lead_id IS NOT NULL) 
    -- OR 
    -- (date_of_reservation::date>date_of_contraction::date or reservation_update_date::date>date_of_contraction::date)


 ),

 duplicates AS --as the id column is not preventing duplication on other cols in the sales_stg table
 (
    SELECT *,
    ROW_NUMBER()  --assuming same lead can't buy multiple properties of the same type in the same location at the same timestamp
    OVER(
        PARTITION BY
             lead_source_id,
             --unit_value,
             unit_location,
             property_type_id,
             area_id,
             compound_id,
             sale_category

              ORDER BY unit_value DESC NULLS LAST,actual_value DESC NULLS LAST,expected_value DESC NULLS LAST,contraction_date DESC  NULLS LAST,years_of_payment DESC  NULLS LAST--,unit_location NULLS DESC LAST
        ) AS rn 

     FROM cleaned_data



    
 )





 SELECT 

    {{ dbt_utils.generate_surrogate_key(['lead_source_id','unit_location','property_type_id','area_id','compound_id','sale_category']) }} AS sale_id,
 original_source_id,
 lead_source_id,
 sale_category,
 area_id,
 compound_id,
 unit_location,
 property_type_id,
 property_type,
 unit_value,
 expected_value,
 actual_value,
 reservation_date,
 reservation_last_update_date,
 contraction_date,
years_of_payment,
is_finalized,
is_orphan_sale,
loaded_at
 
 
  FROM duplicates 
  WHERE rn = 1
