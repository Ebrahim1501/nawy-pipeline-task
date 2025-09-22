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



SELECT DISTINCT ON(normalized_campaign)original_source_campaign,normalized_campaign,CASE
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
            '(cooing [0-9]+|gaa[1-9]|nawy)', 
            1, 1, 'i'
        ) AS tag,


--CASE
--WHEN
(
    CASE
            WHEN 
                LOWER(original_source_campaign) LIKE 'dev - %' OR 
                LOWER(original_source_campaign) LIKE 'dev_-_%' OR 
                LOWER(original_source_campaign) LIKE 'discover_-_dev_-_%'
           
           
            THEN 
                
            SPLIT_PART(
                SPLIT_PART(
                    SPLIT_PART(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    REPLACE(
                                        REGEXP_SUBSTR(
                                            LOWER(TRIM(original_source_campaign)),
                                            '(?:dev_-_|dev - |discover_-_dev_-_)(.*?)(?:_-_| - |$)',
                                            1,
                                            1,
                                            'i'
                                        ),
                                        'discover_-_dev-_',
                                        ''
                                    ),
                                    'dev_-_',
                                    ''
                                ),
                                'dev-',
                                ''
                            ),
                            'discover_-_',
                            ''
                        ),
                        '_-_',
                        1
                    ),
                    '_vbb',
                    1
                ),
                '_ar',
                1
            )
        

                WHEN 
                original_source_campaign LIKE '% -%- %' OR
                original_source_campaign LIKE '%--%--%'
            THEN

                SPLIT_PART(REPLACE(original_source_campaign, '--', ' - '), ' - ', 1)

    END
)


    AS developer_name



    FROM normalized
 )
 SELECT {{dbt_utils.generate_surrogate_key(['original_source_campaign'])}} AS campaign_id,
 original_source_campaign,
 "type",
 tag,
 TRIM(
     REGEXP_REPLACE(
        developer_name,
        '[^a-z ]',   
                ' ',
        'g'
    )
     )
    as developer

 FROM final