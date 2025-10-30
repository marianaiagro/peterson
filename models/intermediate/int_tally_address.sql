WITH expenses_customer_names AS (
    SELECT DISTINCT
          NULLIF(TRIM(party_name), '') AS party_name
    FROM {{ ref('stg_tally_expenses_data') }}
    WHERE party_name IS NOT NULL
)

, voucher_customer_address AS (
    SELECT DISTINCT
          NULLIF(TRIM(customer_name), '') AS customer_name
        , NULLIF(TRIM(address_line_1), '') AS address_line_1
        , NULLIF(TRIM(address_line_2), '') AS address_line_2
        , NULLIF(TRIM(address_line_3), '') AS address_line_3
        , NULLIF(TRIM(address_line_4), '') AS address_line_4
        , NULLIF(TRIM(address_line_5), '') AS address_line_5
    FROM {{ ref('stg_tally_voucher_data') }}
    WHERE customer_name IS NOT NULL
)

, source_data AS (
    SELECT DISTINCT
          party_name
        , address_line_1
        , address_line_2
        , address_line_3
        , address_line_4
        , address_line_5
    FROM expenses_customer_names
    INNER JOIN voucher_customer_address
        ON LOWER(TRIM(expenses_customer_names.party_name)) = LOWER(TRIM(voucher_customer_address.customer_name))
)

, enriched AS (
    SELECT DISTINCT
          party_name
        , address_line_1
        , address_line_2
        , address_line_3
        , address_line_4
        , address_line_5
        , {{ word_tags("address_line_1") }} AS address_line_1_word_tags
        , {{ word_tags("address_line_2") }} AS address_line_2_word_tags
        , {{ word_tags("address_line_3") }} AS address_line_3_word_tags
        , {{ word_tags("address_line_4") }} AS address_line_4_word_tags
        , {{ word_tags("address_line_5") }} AS address_line_5_word_tags
        , TRIM(CONCAT_WS(' ',
              COALESCE(address_line_1, ''),
              COALESCE(address_line_2, ''),
              COALESCE(address_line_3, ''),
              COALESCE(address_line_4, ''),
              COALESCE(address_line_5, '')
          )) AS address_full_original
        , {{ word_tags("address_full_original") }} AS address_full_word_tags
    FROM source_data
)

SELECT DISTINCT
      enriched.party_name
    , enriched.address_full_original
    , enriched.address_full_word_tags
    , country.code AS country_code
    , country.country_en
FROM enriched
LEFT JOIN {{ ref('country') }} AS country
    ON POSITION(LOWER(country.country_en) IN LOWER(enriched.address_full_word_tags)) > 0