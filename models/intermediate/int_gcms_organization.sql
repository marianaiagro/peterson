WITH stg_gcms_companies_src AS (
    SELECT
          client_number
        , name                                   AS legal_name
        , trade_name                             AS name
        , economic_group
        , parent_company
        , country                                AS country_en
        , country_code
        , {{ literal_name("name") }}             AS gcms_literal_name
        , {{ word_tags("name") }}                AS word_tags
        , source_companies
    FROM {{ ref('stg_gcms_companies') }}
)

SELECT
      client_number
    , legal_name
    , name
    , economic_group
    , parent_company
    , country_en
    , country_code
    , gcms_literal_name
    , word_tags
    , source_companies
FROM stg_gcms_companies_src