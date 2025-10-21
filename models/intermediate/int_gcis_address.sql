
WITH address_src AS (

    SELECT
          CAST(stg_gcis_companies.company_id AS VARCHAR) AS client_number
        , stg_gcis_companies.address_street AS address_street_original
        , {{ word_tags("stg_gcis_companies.address_street") }} AS address_street_word_tags
        , stg_gcis_companies.address_number AS address_number_original
        , {{ word_tags("stg_gcis_companies.address_number") }} AS address_number_word_tags
        , stg_gcis_companies.address_city AS address_city_original
        , {{ word_tags("stg_gcis_companies.address_city") }} AS address_city_word_tags
        , stg_gcis_companies.address_state AS address_state_original
        , {{ word_tags("stg_gcis_companies.address_state") }} AS address_state_word_tags
        , stg_gcis_companies.address_country AS address_country_original
        , {{ word_tags("stg_gcis_companies.address_country") }} AS address_country_word_tags
        , stg_gcis_companies.address_zipcode AS address_zipcode_original
        , {{ word_tags("stg_gcis_companies.address_zipcode") }} AS address_zipcode_word_tags
    FROM {{ ref('stg_gcis_companies') }}

), zip_code_src AS (

    SELECT
          country_code
        , postal_code
        , place_name
        , {{ word_tags("place_name") }} AS place_name_word_tags
        , admin_name1
        , admin_code1
        , admin_name2
        , admin_code2
        , admin_name3
        , admin_code3
        , accuracy
    FROM {{ ref('stg_adhoc_country_zip_code') }}

)

SELECT
      address_src.client_number
    , address_src.address_city_word_tags
    , address_src.address_zipcode_word_tags
    , UPPER(zip_code_src.country_code) AS country_code
    , zip_code_src.postal_code AS adhoc_postal_code
    , zip_code_src.place_name AS adhoc_city
    , zip_code_src.place_name_word_tags AS adhoc_city_word_tags
FROM address_src
INNER JOIN zip_code_src
    ON TRIM(address_src.address_zipcode_word_tags) = TRIM(zip_code_src.postal_code)
   AND TRIM(address_src.address_city_word_tags) = TRIM(zip_code_src.place_name_word_tags)