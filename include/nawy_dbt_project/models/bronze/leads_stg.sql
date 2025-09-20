{{ config(materialized='table') }}


SELECT  *,NOW()::TIMESTAMP AS loaded_at FROM {{ref('de_leads')}} --reads from the leads_seed



