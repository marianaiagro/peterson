
SELECT
    country_code
    , postal_code
    , place_name
    , admin_name1
    , admin_code1
    , admin_name2
    , admin_code2
    , admin_name3
    , admin_code3
    , accuracy
FROM {{ source('adhoc', 'country_zip_code') }} AS country_zip_code