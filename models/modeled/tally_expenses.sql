WITH expenses AS (
    SELECT
          expense_id
        , customer_name
        , ledger_name
        , costcenter_name
        , cost_category
        , voucher_id
        , voucher_type
        , voucher_number
        , voucher_date
        , costcenter_amount
        , voucher_amount
        , voucher_total_amount
        , literal_ledger_name
        , literal_costcenter_name
        , literal_cost_category
    FROM {{ ref('int_tally_expenses') }}
)

, costcenter AS (
    SELECT
          ledger_name
        , costcenter_name
        , cost_category
        , group_services
        , literal_ledger_name
        , literal_costcenter_name
        , literal_cost_category
    FROM {{ ref('int_tally_costcenter') }}
)

SELECT DISTINCT
      expenses.expense_id
    , expenses.customer_name
    , expenses.voucher_id
    , expenses.voucher_type
    , expenses.voucher_number
    , expenses.voucher_date
    , LOWER(expenses.ledger_name) AS ledger_name
    , LOWER(expenses.cost_category) AS cost_category
    , LOWER(expenses.costcenter_name) AS costcenter_name
    , costcenter.group_services
    , expenses.voucher_amount
    , expenses.costcenter_amount
    , expenses.voucher_total_amount
FROM expenses
LEFT JOIN costcenter
    ON expenses.literal_ledger_name = costcenter.literal_ledger_name
 AND expenses.literal_costcenter_name = costcenter.literal_costcenter_name
 AND expenses.literal_cost_category = costcenter.literal_cost_category