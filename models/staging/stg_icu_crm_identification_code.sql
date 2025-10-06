/*
------------------------------------------------------------------------------
Table Name        : stg_icu_identification_code
Owner             : iAgro
Created On        : 2025-10-02
Brief Description : Staging view for ICU Identification Code data.
                    Brings all records from the source table 'icu.identification_code'
                    for downstream transformations.
------------------------------------------------------------------------------
*/

WITH source_data AS (

    SELECT
        id
        ,code
        ,type
        ,party_id
        ,data_matches
        ,MAX(_peerdb_synced_at)                 AS _peerdb_synced_at
    FROM {{ source('icu_crm', 'identification_code') }} AS identification_code
    WHERE _peerdb_is_deleted = FALSE
    GROUP BY
        id
        ,code
        ,type
        ,party_id
        ,data_matches 
)

SELECT
    *
FROM source_data