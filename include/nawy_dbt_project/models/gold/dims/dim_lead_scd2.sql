

SELECT --just re-selecting the snapshot scd2 columns with convential scd2 names 
dbt_scd_id as lead_scd_id,
lead_id,
original_source_id,
status_name,
budget,
is_buyer,
is_seller,
is_commercial,
meeting_flag,
last_contact_date,
best_time_to_call,
prefered_location,
dbt_valid_from as effective_date,
dbt_valid_to as expiration_date,
dbt_valid_to IS  NULL AS is_active

FROM {{ref('leads_dim_snapshot')}}

