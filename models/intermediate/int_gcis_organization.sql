WITH gcis_name_normalized AS (
    SELECT
          CAST(stg_gcis_companies.company_id AS VARCHAR)                        AS client_number
        , stg_gcis_companies.company_name                                       AS legal_name
        , stg_gcis_companies.vat                                                AS tax_number
        , stg_gcis_companies.company_group                                      AS company_group
        , LOWER(
              TRANSLATE(
                  CASE 
                      WHEN CHARINDEX('[', stg_gcis_companies.company_name) > 0
                          THEN SUBSTR(stg_gcis_companies.company_name, 1, CHARINDEX('[', stg_gcis_companies.company_name) - 1)
                      ELSE stg_gcis_companies.company_name
                  END
                , ' .,-_()'
                , ''
              )
          )                                                                      AS gcis_literal_name
        , REPLACE(
              TRIM(
                  LOWER(
                      TRANSLATE(
                          CASE 
                              WHEN CHARINDEX('[', stg_gcis_companies.company_name) > 0
                                  THEN SUBSTR(stg_gcis_companies.company_name, 1, CHARINDEX('[', stg_gcis_companies.company_name) - 1)
                              ELSE stg_gcis_companies.company_name
                          END
                        , '.,-_()/]'
                        , ''
                      )
                  )
              )
            , ' '
            , ','
          )                                                                      AS word_tags
    FROM {{ ref('stg_gcis_companies') }} AS stg_gcis_companies
)

SELECT
      client_number
    , legal_name
    , tax_number
    , company_group
    , gcis_literal_name
    , word_tags
FROM gcis_name_normalized