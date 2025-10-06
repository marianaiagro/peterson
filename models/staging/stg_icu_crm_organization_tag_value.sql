/*
------------------------------------------------------------------------------
Table Name        : stg_icu_organization_tag_value
Owner             : iAgro
Created On        : 2025-10-02
Brief Description : Staging view for ICU Organization Tag Value data.
                    Brings all records from the source table 'icu.organization_tag_value'
                    for downstream transformations.
------------------------------------------------------------------------------
*/

WITH source_data AS (

    SELECT
        id
        ,tag_value
        ,organization_tag_id
        ,MAX(_peerdb_synced_at)                 AS _peerdb_synced_at
    FROM {{ source('icu_crm', 'organization_tag_value') }} AS organization_tag_value
    WHERE _peerdb_is_deleted = FALSE
    GROUP BY
        id
        ,tag_value
        ,organization_tag_id
)

SELECT
    *
FROM source_data