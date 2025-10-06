WITH source_data AS (

    SELECT
        id
        ,created_at
        ,language_code
        ,modified_at
        ,picture_id
        ,created_by
        ,modified_by
        ,MAX(_peerdb_synced_at)                 AS _peerdb_synced_at
    FROM {{ source('icu_crm', 'party') }} AS party
    WHERE _peerdb_is_deleted = FALSE
    GROUP BY
         id
        ,created_at
        ,language_code
        ,modified_at
        ,picture_id
        ,created_by
        ,modified_by
)

SELECT
    *
FROM source_data