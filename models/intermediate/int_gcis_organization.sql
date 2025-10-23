WITH source_data AS (
    SELECT
          CAST(stg_gcis_companies.company_id AS VARCHAR)                  AS client_number
        , stg_gcis_companies.company_name                                 AS legal_name
        , stg_gcis_companies.vat                                          AS tax_number
        , stg_gcis_companies.company_group                                AS company_group
        , {{ literal_name("stg_gcis_companies.company_name") }}           AS gcis_literal_name
        , {{ word_tags("stg_gcis_companies.company_name") }}              AS word_tags
        , {{ literal_name("stg_gcis_companies.vat") }}                    AS gcis_tax_number_literal
    FROM {{ ref('stg_gcis_companies') }} AS stg_gcis_companies
)

, address_enriched AS (
    SELECT
          client_number
        , country_en
        , country_code
    FROM {{ ref('int_gcis_address') }}
)

SELECT
      source_data.client_number
    , source_data.legal_name
    , source_data.tax_number
    , source_data.company_group
    , source_data.gcis_literal_name
    , source_data.word_tags
    , source_data.gcis_tax_number_literal
    , address_enriched.country_en
    , address_enriched.country_code
FROM source_data
LEFT JOIN address_enriched
  ON source_data.client_number = address_enriched.client_number