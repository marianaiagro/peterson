
WITH tag_list AS (
    SELECT 
        id
        ,name
        ,organization_id
    FROM {{ ref('stg_icu_organization_tag') }}
),

tag_values AS (
    SELECT 
        id
        ,tag_value
        ,organization_tag_id
    FROM {{ ref('stg_icu_organization_tag_value') }}
)

SELECT
    tag_list.organization_id
    ,MAX(DECODE(tag_list.name, 'type'      , LOWER(TRIM(tag_values.tag_value)), NULL)) AS organization_type
    ,MAX(DECODE(tag_list.name, 'status'    , LOWER(TRIM(tag_values.tag_value)), NULL)) AS status
    ,MAX(DECODE(tag_list.name, 'system'    , LOWER(TRIM(tag_values.tag_value)), NULL)) AS organization_system
    ,MAX(DECODE(tag_list.name, 'isMigrated', LOWER(TRIM(tag_values.tag_value)), NULL)) AS brand
    ,MAX(DECODE(tag_list.name, 'founder'   , LOWER(TRIM(tag_values.tag_value)), NULL)) AS founder
    ,MAX(DECODE(tag_list.name, 'holding'   , LOWER(TRIM(tag_values.tag_value)), NULL)) AS holding
FROM tag_list
INNER JOIN tag_values
  ON tag_list.id = tag_values.organization_tag_id
GROUP BY tag_list.organization_id