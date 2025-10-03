
WITH source_data AS (
    SELECT
      TRY_TO_NUMBER(TO_VARCHAR(raw:"id"))                               AS commodity_id
    , TRY_TO_NUMBER(TO_VARCHAR(raw:"instanceId"))                       AS instance_id
    , raw:"name"::STRING                                                AS commodity_name
    , raw:"localName"::STRING                                           AS commodity_local_name
    , raw:"category"::STRING                                            AS commodity_category
    , raw:"cargoType"::STRING                                           AS cargo_type
    , raw:"hsCode"::STRING                                              AS hs_code
    , raw:"shortCode"::STRING                                           AS short_code
    , IFF(UPPER(raw:"global"::STRING) IN ('YES','Y','TRUE'),1,0)        AS is_global
    FROM {{ source('gcis','gcis_commodities') }}
)

SELECT
    *
FROM source_data
