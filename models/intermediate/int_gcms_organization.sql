<<<<<<< HEAD
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
=======

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
>>>>>>> d2d9a082da8111719f22f8571285f1a951b477e8
        , source_companies
    FROM {{ ref('stg_gcms_companies') }}
)

SELECT
<<<<<<< HEAD
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
=======
    DISTINCT *
FROM stg_gcms_companies
>>>>>>> d2d9a082da8111719f22f8571285f1a951b477e8
