/*
------------------------------------------------------------------------------
Table Name        : stg_icu_identification_code
Owner             : iAgro
Created On        : 2025-10-02
Brief Description : 
------------------------------------------------------------------------------
*/

WITH source_data AS (
    SELECT
        id
      , code
      , type
      , party_id
      , data_matches
      , MAX(_peerdb_synced_at) AS _peerdb_synced_at
    FROM {{ source('icu_crm', 'identification_code') }} AS identification_code
    WHERE _peerdb_is_deleted = FALSE
    GROUP BY id, code, type, party_id, data_matches
)

SELECT
      CAST(id           AS VARCHAR)    AS id
    , CAST(code         AS VARCHAR)    AS code
    , CAST(type         AS VARCHAR)    AS type
    , CAST(party_id     AS VARCHAR)    AS party_id
    , data_matches                     AS data_matches
    , _peerdb_synced_at                AS _peerdb_synced_at
FROM source_data
