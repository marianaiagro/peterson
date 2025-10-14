
SELECT
    client_number
    , name                          AS legal_name
    , trade_name                    AS name
    , economic_group
    , parent_company
    , complement_type
    , complement_value
    , country_code
    , LOWER(
          TRANSLATE(
              CASE 
                  WHEN CHARINDEX('[', name) > 0
                      THEN SUBSTR(name, 1, CHARINDEX('[', name) - 1)
                  ELSE name
              END
            , ' .,-_()'
            , ''
          )
      ) AS gcms_literal_name
    , REPLACE(
          LOWER(
              TRANSLATE(
                  CASE 
                      WHEN CHARINDEX('[', name) > 0
                          THEN SUBSTR(name, 1, CHARINDEX('[', name) - 1)
                      ELSE name
                  END
                , '.,-_()/]'
                , ''
              )
          )
        , ' '
        , ','
      ) AS word_tags
FROM {{ ref('stg_gcms_companies') }}
