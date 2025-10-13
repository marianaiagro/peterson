/*
------------------------------------------------------------------------------
Table Name        : stg_icu_organization_tag
Owner             : iAgro
Created On        : 2025-10-02
Brief Description : 
------------------------------------------------------------------------------
*/

WITH source_data AS (
    SELECT
        id
      , name
      , organization_id
      , MAX(_peerdb_synced_at) AS _peerdb_synced_at
    FROM {{ source('icu_crm', 'organization_tag') }} AS organization_tag
    WHERE _peerdb_is_deleted = FALSE
    GROUP BY id, name, organization_id
)

SELECT
      CAST(id               AS VARCHAR)   AS id
    , CAST(name             AS VARCHAR)   AS name
    , CAST(organization_id  AS VARCHAR)   AS organization_id
    , _peerdb_synced_at                   AS _peerdb_synced_at
FROM source_data
