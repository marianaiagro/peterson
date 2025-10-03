WITH source_data AS (

    SELECT
        id
        ,created_at
        ,language_code
        ,modified_at
        ,picture_id
        ,created_by
        ,modified_by
        ,_peerdb_is_deleted
        ,_peerdb_synced_at
    FROM {{ source('icu', 'party') }} AS party

)

SELECT
    *
FROM source_data