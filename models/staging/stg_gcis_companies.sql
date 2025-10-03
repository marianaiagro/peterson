
WITH source_data AS (
    SELECT
        TRY_TO_NUMBER(TO_VARCHAR(raw:"id"))                                 AS company_id
        , TRY_TO_NUMBER(TO_VARCHAR(raw:"instanceId"))                         AS instance_id
        , raw:"name"::STRING                                                  AS company_name
        , raw:"vat"::STRING                                                   AS vat
        , raw:"group"::STRING                                                 AS company_group
        , IFF(UPPER(raw:"global"::STRING) IN ('YES','Y','TRUE'), 1, 0)        AS is_global
        , raw:"address"."street"::STRING                                      AS address_street
        , raw:"address"."number"::STRING                                      AS address_number
        , raw:"address"."city"::STRING                                        AS address_city
        , raw:"address"."state"::STRING                                       AS address_state
        , raw:"address"."country"::STRING                                     AS address_country
        , raw:"address"."zipCode"::STRING                                     AS address_zipcode
    FROM {{ source('gcis','gcis_companies') }}
)

SELECT
    *
FROM source_data
