WITH expenses_customer_names AS (
    SELECT DISTINCT
          NULLIF(TRIM(party_name), '') AS party_name
    FROM {{ ref('stg_tally_expenses_data') }}
    WHERE party_name IS NOT NULL
)

, voucher_costcenter_customer_name AS (
    SELECT DISTINCT
          NULLIF(TRIM(customer_name), '') AS customer_name
    FROM {{ ref('stg_tally_voucher_data') }}
    WHERE customer_name IS NOT NULL

    UNION

    SELECT DISTINCT
          NULLIF(TRIM(customer_name), '') AS customer_name
    FROM {{ ref('stg_tally_voucher_costcenter_data') }}
    WHERE customer_name IS NOT NULL
)

, source_data AS (
    SELECT DISTINCT
          party_name
    FROM expenses_customer_names
    INNER JOIN voucher_costcenter_customer_name
        ON LOWER(expenses_customer_names.party_name) = LOWER(voucher_costcenter_customer_name.customer_name)
)

, address_data AS (
    SELECT DISTINCT
          party_name
        , address_full_original
        , address_full_word_tags
        , country_code
        , country_en
    FROM {{ ref('int_tally_address') }}
)

SELECT DISTINCT
      source_data.party_name                          AS legal_name
    , {{ literal_name("source_data.party_name") }}    AS tally_literal_name
    , {{ word_tags("source_data.party_name") }}       AS word_tags
    , address_data.address_full_original
    , address_data.address_full_word_tags
    , address_data.country_code
    , address_data.country_en
FROM source_data
LEFT JOIN address_data
    ON LOWER(TRIM(source_data.party_name)) = LOWER(TRIM(address_data.party_name))