
WITH source_data AS (
    SELECT
          stg_icu_organization.client_number          AS client_number
        , stg_icu_organization.legal_name             AS legal_name
        , stg_icu_organization.legal_number           AS legal_number
        , stg_icu_organization.name                   AS name
        , stg_icu_organization.tax_number             AS tax_number
        , stg_icu_organization.website                AS website
        , stg_icu_organization.id                     AS organization_id
        , stg_icu_organization.approved               AS approved
        , stg_icu_organization.approved_at            AS approved_at
        , int_organization_tag.organization_type      AS organization_type
        , int_organization_tag.status                 AS status
        , int_organization_tag.organization_system    AS organization_system
        , int_organization_tag.brand                  AS brand
        , int_organization_tag.founder                AS founder
        , int_organization_tag.holding                AS holding
        , LOWER(
              TRANSLATE(
                  CASE 
                      WHEN CHARINDEX('[', stg_icu_organization.legal_name) > 0
                          THEN SUBSTR(stg_icu_organization.legal_name, 1, CHARINDEX('[', stg_icu_organization.legal_name) - 1)
                      ELSE stg_icu_organization.legal_name
                  END,
                  ' .,-_()',
                  ''
              )
          )                                           AS icu_literal_name
        , REPLACE(
              TRIM(
                  LOWER(
                      TRANSLATE(
                          CASE 
                              WHEN CHARINDEX('[', stg_icu_organization.legal_name) > 0
                                  THEN SUBSTR(stg_icu_organization.legal_name, 1, CHARINDEX('[', stg_icu_organization.legal_name) - 1)
                              ELSE stg_icu_organization.legal_name
                          END,
                          '.,-_()/]',
                          ''
                      )
                  )
              ),
              ' ',
              ','
          )                                           AS word_tags
    FROM {{ ref('stg_icu_crm_organization') }} AS stg_icu_organization
    INNER JOIN {{ ref('int_icu_organization_tag') }} AS int_organization_tag
        ON stg_icu_organization.id = int_organization_tag.organization_id
),

seed_country AS (
    SELECT
          code
        , country_en
        , country_es
    FROM {{ ref('country') }}
),

address AS (
    SELECT
          stg_icu_address.party_id
        , stg_icu_address.city
        , stg_icu_address.country_iso_code
        , stg_icu_address.state_iso_code
        , stg_icu_address.type
        , stg_icu_address.zip
    FROM {{ ref('stg_icu_crm_address') }} AS stg_icu_address
    WHERE is_primary
)

SELECT
      source_data.client_number
    , source_data.legal_name
    , source_data.legal_number
    , source_data.name
    , source_data.tax_number
    , source_data.website
    , source_data.organization_id
    , source_data.approved
    , source_data.approved_at
    , source_data.organization_type
    , source_data.status
    , source_data.organization_system
    , source_data.brand
    , source_data.founder
    , source_data.holding
    , source_data.icu_literal_name
    , source_data.word_tags
    , seed_country.country_en                           AS country_en
    , address.country_iso_code                          AS country_code
FROM source_data
LEFT JOIN address
    ON source_data.organization_id = address.party_id
LEFT JOIN seed_country
    ON seed_country.code = address.country_iso_code