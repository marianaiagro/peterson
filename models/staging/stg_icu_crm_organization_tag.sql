/*
------------------------------------------------------------------------------
Table Name        : stg_icu_organization_tag
Owner             : iAgro
Created On        : 2025-10-02
Brief Description : Staging view for ICU Organization Tag data.
                    Brings all records from the source table 'icu.organization_tag'
                    for downstream transformations.
------------------------------------------------------------------------------
*/

WITH source_data AS (

    SELECT
        id
        ,name
        ,organization_id
        ,MAX(_peerdb_synced_at)                 AS _peerdb_synced_at
    FROM {{ source('icu_crm', 'organization_tag') }} AS organization_tag
    WHERE _peerdb_is_deleted = FALSE
    GROUP BY
        id
        ,name
        ,organization_id
)

SELECT
    *
FROM source_data