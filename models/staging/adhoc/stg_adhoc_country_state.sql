SELECT
    c.code                                    AS country_code
    , c.country_en                              AS country_en
    , c.country_es                              AS country_es
    , sp.state_province_code                    AS state_province_code
    , sp.state_province                         AS state_province
FROM {{ ref('state_province') }} AS sp
LEFT JOIN {{ ref('country') }} AS c
    ON sp.uuid_country = c.uuid