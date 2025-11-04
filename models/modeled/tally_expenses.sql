WITH expenses AS (
    SELECT
          customer_name
        , costcenter_name
        , id                  AS voucher_id
        , voucher_type
        , voucher_number
        , voucher_date
        , costcenter_amount
        , voucher_total_amount
        , literal_costcenter_name
    FROM {{ ref('int_tally_expenses') }}
)

, costcenter AS (
    SELECT
          ledger_name
        , costcenter_name
        , cost_category
        , group_services
        , literal_costcenter_name
    FROM {{ ref('int_tally_costcenter') }}
)

SELECT
    expenses.customer_name
    , expenses.voucher_id
    , expenses.voucher_type
    , expenses.voucher_number
    , expenses.voucher_date
    , expenses.costcenter_name
    , costcenter.cost_category
    , costcenter.ledger_name
    , costcenter.group_services
    , expenses.costcenter_amount
    , expenses.voucher_total_amount
FROM expenses
LEFT JOIN costcenter
  ON expenses.literal_costcenter_name = costcenter.literal_costcenter_name
