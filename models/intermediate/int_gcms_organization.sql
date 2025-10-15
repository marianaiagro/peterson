
WITH stg_gcms_companies AS (
    SELECT
        client_number
        , name                          AS legal_name
        , trade_name                    AS name
        , economic_group
        , parent_company
        , country                       AS country_en
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
        , source_companies
    FROM {{ ref('stg_gcms_companies') }}
)

SELECT
    DISTINCT *
FROM stg_gcms_companies