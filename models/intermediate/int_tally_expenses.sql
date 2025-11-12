WITH voucher_costcenter_data AS (
    SELECT
          voucher_id
        , customer_name
        , costcenter_name
        , voucher_type
        , voucher_number
        , voucher_date
        , ledger_name
        , costcenter_amount
        , cost_category
        , {{ literal_name("customer_name") }}      AS literal_customer_name
        , {{ literal_name("ledger_name") }}      AS literal_ledger_name
        , {{ literal_name("costcenter_name") }}  AS literal_costcenter_name
        , {{ literal_name("cost_category") }}    AS literal_cost_category
    FROM {{ ref('stg_tally_voucher_costcenter_data') }} AS voucher_costcenter_data
    WHERE customer_name IS NOT NULL
)

, voucher_data AS (
    SELECT
          voucher_id
        , ledger_name
        , amount
        , voucher_total_amount
        , {{ literal_name("ledger_name") }}      AS literal_ledger_name
    FROM {{ ref('stg_tally_voucher_data') }} AS voucher_data
)

, expenses_data AS (
    SELECT
          expense_id
        , voucher_id
        , ledger_name
        , costcenter_name
        , costcenter_amount
        , total_amount
        , cost_category
        , {{ literal_name("ledger_name") }}      AS literal_ledger_name
        , {{ literal_name("costcenter_name") }}  AS literal_costcenter_name
        , {{ literal_name("cost_category") }}    AS literal_cost_category
    FROM {{ ref('stg_tally_expenses_data') }} AS expenses_data
)

SELECT
      expenses_data.expense_id
    , voucher_costcenter_data.customer_name
    , voucher_costcenter_data.ledger_name
    , voucher_costcenter_data.costcenter_name
    , voucher_costcenter_data.cost_category
    , voucher_costcenter_data.voucher_id
    , voucher_costcenter_data.voucher_type
    , voucher_costcenter_data.voucher_number
    , voucher_costcenter_data.voucher_date
    , voucher_data.amount                         AS voucher_amount
    , voucher_costcenter_data.costcenter_amount   AS costcenter_amount
    , voucher_data.voucher_total_amount           AS voucher_total_amount
    , voucher_costcenter_data.literal_customer_name AS literal_customer_name
    , voucher_costcenter_data.literal_ledger_name AS literal_ledger_name
    , voucher_costcenter_data.literal_costcenter_name AS literal_costcenter_name
    , voucher_costcenter_data.literal_cost_category   AS literal_cost_category
FROM voucher_costcenter_data
INNER JOIN expenses_data
  ON expenses_data.voucher_id = voucher_costcenter_data.voucher_id
 AND expenses_data.literal_ledger_name = voucher_costcenter_data.literal_ledger_name
 AND expenses_data.literal_costcenter_name = voucher_costcenter_data.literal_costcenter_name
 AND expenses_data.costcenter_amount = voucher_costcenter_data.costcenter_amount
 AND expenses_data.literal_cost_category = voucher_costcenter_data.literal_cost_category
INNER JOIN voucher_data
  ON voucher_data.voucher_id = voucher_costcenter_data.voucher_id
 AND voucher_data.literal_ledger_name = voucher_costcenter_data.literal_ledger_name
 AND voucher_data.amount = ABS(voucher_costcenter_data.costcenter_amount)
 AND voucher_data.voucher_total_amount = expenses_data.total_amount
WHERE voucher_costcenter_data.customer_name IS NOT NULL