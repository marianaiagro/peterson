
WITH source_data AS (
    SELECT
        id
      , created_at
      , language_code
      , modified_at
      , picture_id
      , created_by
      , modified_by
      , MAX(_peerdb_synced_at) AS _peerdb_synced_at
    FROM {{ source('icu_crm', 'party') }} AS party
    WHERE _peerdb_is_deleted = FALSE
    GROUP BY id, created_at, language_code, modified_at, picture_id, created_by, modified_by
)

SELECT
      CAST(id                               AS VARCHAR)   AS id
    , created_at                                          AS created_at
    , CAST(language_code                    AS VARCHAR)   AS language_code
    , modified_at                                         AS modified_at
    , CAST(picture_id                       AS VARCHAR)   AS picture_id
    , CAST(created_by                       AS VARCHAR)   AS created_by
    , CAST(modified_by                      AS VARCHAR)   AS modified_by
    , _peerdb_synced_at                                   AS _peerdb_synced_at
FROM source_data
