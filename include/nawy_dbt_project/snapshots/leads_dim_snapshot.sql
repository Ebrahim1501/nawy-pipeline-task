{% snapshot leads_dim_snapshot %}

{{
    config(
      strategy='timestamp',
      unique_key='lead_id',
      updated_at='updated_at'
    )
}}

SELECT  lead_id,
        original_source_id, --not all lead columns only the ones to track their changes
        status_name ,
        budget,
        is_buyer,
        is_seller,
        is_commercial,
        last_contact_date,
        best_time_to_call,
        meeting_flag,
        prefered_location,
     
--        created_at, 
        updated_at --used by dbt to track changes (not actually included in the final dim )
     
        FROM {{ref('leads_transformed')}}




{% endsnapshot %}
