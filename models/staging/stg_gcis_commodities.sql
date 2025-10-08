
WITH source_data AS (
    SELECT
      TRY_TO_NUMBER(TO_VARCHAR(raw:"id"))                                                                                           AS commodity_id
      ,TRY_TO_NUMBER(TO_VARCHAR(raw:"instanceId"))                                                                                  AS instance_id
      ,IFF(raw:"name" IS NULL OR UPPER(TRIM(raw:"name"::STRING)) = 'NULL', NULL, raw:"name"::STRING)                                AS commodity_name
      ,IFF(raw:"localName" IS NULL OR UPPER(TRIM(raw:"localName"::STRING)) = 'NULL', NULL, raw:"localName"::STRING)                 AS commodity_local_name
      ,IFF(raw:"category" IS NULL OR UPPER(TRIM(raw:"category"::STRING)) = 'NULL', NULL, raw:"category"::STRING)                    AS commodity_category
      ,IFF(raw:"cargoType" IS NULL OR UPPER(TRIM(raw:"cargoType"::STRING)) = 'NULL', NULL, raw:"cargoType"::STRING)                 AS cargo_type
      ,IFF(raw:"hsCode" IS NULL OR UPPER(TRIM(raw:"hsCode"::STRING)) = 'NULL', NULL, raw:"hsCode"::STRING)                          AS hs_code
      ,IFF(raw:"shortCode" IS NULL OR UPPER(TRIM(raw:"shortCode"::STRING)) = 'NULL', NULL, raw:"shortCode"::STRING)                 AS short_code
      ,CASE
        WHEN raw:"global" IS NULL OR UPPER(TRIM(raw:"global"::STRING)) = 'NULL' THEN NULL
        WHEN UPPER(raw:"global"::STRING) IN ('YES','Y','TRUE') THEN 1
        ELSE 0
      END                                                                                                                          AS is_global
    FROM {{ source('gcis','gcis_commodities') }}
)

SELECT *
FROM source_data