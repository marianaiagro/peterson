/*
------------------------------------------------------------------------------
Table Name        : stg_icu_organization_tag_value
Owner             : iAgro
Created On        : 2025-10-02
Brief Description : 
------------------------------------------------------------------------------
*/

WITH source_data AS (
    SELECT
        id
      , tag_value
      , organization_tag_id
      , MAX(_peerdb_synced_at) AS _peerdb_synced_at
    FROM {{ source('icu_crm', 'organization_tag_value') }} AS organization_tag_value
    WHERE _peerdb_is_deleted = FALSE
    GROUP BY id, tag_value, organization_tag_id
)

SELECT
      CAST(id                       AS VARCHAR)     AS id
    , CAST(tag_value                AS VARCHAR)     AS tag_value
    , CAST(organization_tag_id      AS VARCHAR)     AS organization_tag_id
    , _peerdb_synced_at                             AS _peerdb_synced_at
FROM source_data
