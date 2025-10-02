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
        ,_peerdb_is_deleted
        ,_peerdb_synced_at
    FROM {{ source('icu', 'organization_tag') }} AS organization_tag

)

SELECT
    *
FROM source_data