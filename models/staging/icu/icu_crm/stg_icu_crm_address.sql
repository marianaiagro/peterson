
WITH source_data AS (
    SELECT
        id
      , city
      , country_iso_code
      , description
      , is_primary
      , latitude
      , line1
      , line2
      , line3
      , longitude
      , state_iso_code
      , type
      , zip
      , bank_account_id
      , party_id
      , MAX(_peerdb_synced_at) AS _peerdb_synced_at
    FROM {{ source('icu_crm', 'address') }} AS address
    WHERE _peerdb_is_deleted = FALSE
    GROUP BY
        id, city, country_iso_code, description, is_primary, latitude, line1, line2, line3
      , longitude, state_iso_code, type, zip, bank_account_id, party_id
)

SELECT
      CAST(id                       AS VARCHAR)   AS id
    , CAST(city                     AS VARCHAR)   AS city
    , CAST(country_iso_code         AS VARCHAR)   AS country_iso_code
    , CAST(description              AS VARCHAR)   AS description
    , CAST(is_primary               AS BOOLEAN)   AS is_primary
    , latitude                                    AS latitude
    , CAST(line1                    AS VARCHAR)   AS line1
    , CAST(line2                    AS VARCHAR)   AS line2
    , CAST(line3                    AS VARCHAR)   AS line3
    , longitude                                   AS longitude
    , CAST(state_iso_code           AS VARCHAR)   AS state_iso_code
    , CAST(type                     AS VARCHAR)   AS type
    , CAST(zip                      AS VARCHAR)   AS zip
    , CAST(bank_account_id          AS VARCHAR)   AS bank_account_id
    , CAST(party_id                 AS VARCHAR)   AS party_id
    , _peerdb_synced_at                           AS _peerdb_synced_at
FROM source_data
