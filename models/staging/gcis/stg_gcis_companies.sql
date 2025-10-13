WITH source_data AS (
    SELECT
        TO_VARCHAR(raw:"id")                                                                                   AS company_id
      , TO_VARCHAR(raw:"instanceId")                                                                           AS instance_id
      , IFF(raw:"name"   IS NULL OR UPPER(TRIM(raw:"name"::STRING))   = 'NULL', NULL, raw:"name"::STRING)      AS company_name
      , IFF(raw:"vat"    IS NULL OR UPPER(TRIM(raw:"vat"::STRING))    = 'NULL', NULL, raw:"vat"::STRING)       AS vat
      , IFF(raw:"group"  IS NULL OR UPPER(TRIM(raw:"group"::STRING))  = 'NULL', NULL, raw:"group"::STRING)     AS company_group
      , CASE
            WHEN raw:"global" IS NULL OR UPPER(TRIM(raw:"global"::STRING)) = 'NULL' THEN NULL
            WHEN UPPER(raw:"global"::STRING) IN ('YES','Y','TRUE') THEN TRUE
            ELSE FALSE
        END::BOOLEAN                                                                                           AS is_global
      , IFF(raw:"address"."street"   IS NULL OR UPPER(TRIM(raw:"address"."street"::STRING))   = 'NULL', NULL, raw:"address"."street"::STRING)    AS address_street
      , IFF(raw:"address"."number"   IS NULL OR UPPER(TRIM(raw:"address"."number"::STRING))   = 'NULL', NULL, raw:"address"."number"::STRING)    AS address_number
      , IFF(raw:"address"."city"     IS NULL OR UPPER(TRIM(raw:"address"."city"::STRING))     = 'NULL', NULL, raw:"address"."city"::STRING)      AS address_city
      , IFF(raw:"address"."state"    IS NULL OR UPPER(TRIM(raw:"address"."state"::STRING))    = 'NULL', NULL, raw:"address"."state"::STRING)     AS address_state
      , IFF(raw:"address"."country"  IS NULL OR UPPER(TRIM(raw:"address"."country"::STRING))  = 'NULL', NULL, raw:"address"."country"::STRING)   AS address_country
      , IFF(raw:"address"."zipCode"  IS NULL OR UPPER(TRIM(raw:"address"."zipCode"::STRING))  = 'NULL', NULL, raw:"address"."zipCode"::STRING)   AS address_zipcode
    FROM {{ source('gcis','gcis_companies') }}
)

SELECT
    *
FROM source_data