with distinct_campaigns as
( 
    SELECT DISTINCT campaign  
    FROM {{ref('leads_transformed')}}
    WHERE campaign IS NOT NULL

)
,
 normalized AS
(
    SELECT 
    campaign as original_source_campaign,
    TRIM(REPLACE(
    REPLACE(
        REPLACE(
            LOWER(campaign), -- First, make the whole string lowercase
            '_', ''
        ),
        '+', ''
    ),
    '.', ''))AS normalized_campaign
 
 FROM distinct_campaigns

 ),
 final AS
 (



SELECT DISTINCT ON(normalized_campaign) original_source_campaign,CASE
        WHEN normalized_campaign ILIKE '%lead%' THEN 'lead generation'
        WHEN normalized_campaign ILIKE '%remarketing%' THEN 'remarketing'
        WHEN normalized_campaign ILIKE '%financing%' THEN 'financing'
        WHEN normalized_campaign ILIKE '%hiring%' THEN 'hiring'
        WHEN normalized_campaign ILIKE '%branding%' THEN 'branding'
        WHEN normalized_campaign ILIKE '%traffic%' THEN 'traffic'
        WHEN normalized_campaign ILIKE '%a/b test%' THEN 'a/b testing'
        WHEN normalized_campaign ILIKE '%pmax%' THEN 'pmax'
        WHEN normalized_campaign ILIKE '%dynamictarget%' THEN 'dynamic targetting'
        ELSE 'other'
    END AS "type",

    REGEXP_SUBSTR(
            normalized_campaign,
            '(cooing [0-9]+|gaa[1-9]|nawy)', -- The pattern we built
            1, 1, 'i'
        ) AS tag
    FROM normalized
 )
 SELECT {{dbt_utils.generate_surrogate_key(['original_source_campaign'])}} AS campaign_id,*
 FROM final