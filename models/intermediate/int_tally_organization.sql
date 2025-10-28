WITH expenses AS (
    SELECT
          costcenter_name
        , remote_id
        , comp_name
        , comp_code
        , party_name
    FROM {{ ref('stg_tally_expenses_data') }}
)
, voucher AS (
    SELECT
          remote_id
        , company_name
        , reference_number
        , customer_name
    FROM {{ ref('stg_tally_voucher_data') }}
)
, costcenter AS (
    SELECT
          remote_id
        , company_name
        , reference_number
        , customer_name
    FROM {{ ref('stg_tally_voucher_costcenter_data') }}
)
, source_data AS (
    SELECT
          COALESCE(expenses.remote_id, voucher.remote_id, costcenter.remote_id) AS remote_id
        , COALESCE(voucher.company_name, costcenter.company_name, expenses.comp_name) AS company_name
        , COALESCE(voucher.customer_name, costcenter.customer_name, expenses.party_name) AS customer_name
        , expenses.comp_code
        , expenses.costcenter_name
    FROM expenses
    FULL OUTER JOIN voucher
        ON expenses.remote_id = voucher.remote_id
    FULL OUTER JOIN costcenter
        ON COALESCE(expenses.remote_id, voucher.remote_id) = costcenter.remote_id
)
SELECT
      source_data.remote_id                                AS client_number
    , source_data.customer_name                            AS legal_name
    , source_data.company_name
    , source_data.comp_code
    , source_data.costcenter_name
    , {{ literal_name("source_data.company_name") }}       AS tally_literal_name
    , {{ word_tags("source_data.company_name") }}          AS word_tags
FROM source_data