{{ config(materialized='table') }}


SELECT DISTINCT *,NOW()::TIMESTAMP AS loaded_at 
FROM {{ref('de_sales')}} --load data from sales seed