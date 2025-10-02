/*
------------------------------------------------------------------------------
Table Name        : stg_icu__organization
Owner             : iAgro
Created On        : 2025-10-01
Brief Description : Staging view for ICU Organization data.
                    Brings all records from the source table 'icu.organization'
                    for downstream transformations.
------------------------------------------------------------------------------
*/

WITH source_data AS (

    SELECT
        client_number
        ,invoice_format
        ,legal_name
        ,legal_number
        ,name
        ,tax_number
        ,type
        ,website
        ,id
        ,status
        ,deleted
        ,time_zone_id
        ,approved
        ,deleted_at
        ,deleted_by
        ,approval_priority
        ,duplicated
        ,duplicated_at
        ,duplicated_by
        ,approved_at
        ,approved_by
        ,number_of_employees
        ,_peerdb_is_deleted
        ,_peerdb_synced_at
    FROM {{ source('icu', 'organization') }} AS organization

)

SELECT
    *
FROM source_data