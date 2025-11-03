WITH expenses_customer_names AS (
    SELECT DISTINCT
          NULLIF(TRIM(party_name), '') AS party_name
    FROM {{ ref('stg_tally_expenses_data') }}
    WHERE party_name IS NOT NULL
)

, voucher_customer_address AS (
    SELECT DISTINCT
          NULLIF(TRIM(customer_name), '')  AS customer_name
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
          expenses_customer_names.party_name
        , voucher_customer_address.address_line_1
        , voucher_customer_address.address_line_2
        , voucher_customer_address.address_line_3
        , voucher_customer_address.address_line_4
        , voucher_customer_address.address_line_5
    FROM expenses_customer_names AS expenses_customer_names
    INNER JOIN voucher_customer_address AS voucher_customer_address
        ON LOWER(TRIM(expenses_customer_names.party_name)) = LOWER(TRIM(voucher_customer_address.customer_name))
)

, enriched_addresses AS (
    SELECT DISTINCT
          source_data_address.party_name
        , source_data_address.address_line_1
        , {{ word_tags("source_data_address.address_line_1") }} AS address_line_1_word_tags
        , source_data_address.address_line_2
        , {{ word_tags("source_data_address.address_line_2") }} AS address_line_2_word_tags
        , source_data_address.address_line_3
        , {{ word_tags("source_data_address.address_line_3") }} AS address_line_3_word_tags
        , source_data_address.address_line_4
        , {{ word_tags("source_data_address.address_line_4") }} AS address_line_4_word_tags
        , source_data_address.address_line_5
        , {{ word_tags("source_data_address.address_line_5") }} AS address_line_5_word_tags
        , TRIM(CONCAT_WS(' '
              , COALESCE(source_data_address.address_line_1, '')
              , COALESCE(source_data_address.address_line_2, '')
              , COALESCE(source_data_address.address_line_3, '')
              , COALESCE(source_data_address.address_line_4, '')
              , COALESCE(source_data_address.address_line_5, '')
          )) AS address_full_original
        , {{ word_tags(
            "concat_ws(' ', " ~
                "coalesce(source_data_address.address_line_1, ''), " ~
                "coalesce(source_data_address.address_line_2, ''), " ~
                "coalesce(source_data_address.address_line_3, ''), " ~
                "coalesce(source_data_address.address_line_4, ''), " ~
                "coalesce(source_data_address.address_line_5, ''))"
          ) }} AS address_full_word_tags
    FROM source_data AS source_data_address
)

, address_lines AS (
    SELECT
          party_name
        , 5 AS line_no
        , address_line_5 AS line_txt
        , address_line_5_word_tags AS line_tags
    FROM enriched_addresses

    UNION ALL

    SELECT
          party_name
        , 4 AS line_no
        , address_line_4 AS line_txt
        , address_line_4_word_tags AS line_tags
    FROM enriched_addresses

    UNION ALL

    SELECT
          party_name
        , 3 AS line_no
        , address_line_3 AS line_txt
        , address_line_3_word_tags AS line_tags
    FROM enriched_addresses

    UNION ALL

    SELECT
          party_name
        , 2 AS line_no
        , address_line_2 AS line_txt
        , address_line_2_word_tags AS line_tags
    FROM enriched_addresses

    UNION ALL

    SELECT
          party_name
        , 1 AS line_no
        , address_line_1 AS line_txt
        , address_line_1_word_tags AS line_tags
    FROM enriched_addresses
)

, state_country_dim AS (
    SELECT DISTINCT
          LOWER(TRIM(country_en)) AS country_en_lc
        , ANY_VALUE(country_code) AS country_code
        , MAX(country_en)         AS country_en
    FROM {{ ref('stg_adhoc_country_state') }}
    WHERE country_en IS NOT NULL
    GROUP BY LOWER(TRIM(country_en))
)

, country_hits AS (
    SELECT
          address_lines_data.party_name
        , address_lines_data.line_no
        , address_lines_data.line_txt
        , address_lines_data.line_tags
        , state_country_data.country_code
        , state_country_data.country_en
        , CASE
            WHEN POSITION(state_country_data.country_en_lc IN LOWER(address_lines_data.line_tags)) > 0 THEN 1
            ELSE 0
          END AS hit
    FROM address_lines AS address_lines_data
    LEFT JOIN state_country_dim AS state_country_data
      ON POSITION(state_country_data.country_en_lc IN LOWER(address_lines_data.line_tags)) > 0
)

, best_country AS (
    SELECT
          party_name
        , line_no AS matched_line_no
        , country_code
        , country_en
    FROM country_hits
    WHERE hit = 1
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY party_name
        ORDER BY line_no DESC
    ) = 1
)

, final_cte AS (
    SELECT DISTINCT
          enriched_addresses_data.party_name
        , enriched_addresses_data.address_line_1
        , enriched_addresses_data.address_line_1_word_tags
        , enriched_addresses_data.address_line_2
        , enriched_addresses_data.address_line_2_word_tags
        , enriched_addresses_data.address_line_3
        , enriched_addresses_data.address_line_3_word_tags
        , enriched_addresses_data.address_line_4
        , enriched_addresses_data.address_line_4_word_tags
        , enriched_addresses_data.address_line_5
        , enriched_addresses_data.address_line_5_word_tags
        , enriched_addresses_data.address_full_original
        , enriched_addresses_data.address_full_word_tags
        , best_country_data.country_code
        , best_country_data.country_en
        , best_country_data.matched_line_no AS matched_from_line
    FROM enriched_addresses AS enriched_addresses_data
    LEFT JOIN best_country AS best_country_data
      ON best_country_data.party_name = enriched_addresses_data.party_name
)

SELECT
      party_name
    , address_line_1
    , address_line_1_word_tags
    , address_line_2
    , address_line_2_word_tags
    , address_line_3
    , address_line_3_word_tags
    , address_line_4
    , address_line_4_word_tags
    , address_line_5
    , address_line_5_word_tags
    , address_full_original
    , address_full_word_tags
    , country_code
    , country_en
    , matched_from_line
FROM final_cte