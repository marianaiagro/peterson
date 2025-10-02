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
        ,_peerdb_is_deleted
        ,_peerdb_synced_at
    FROM {{ source('icu', 'identification_code') }} AS identification_code

)

SELECT
    *
FROM source_data