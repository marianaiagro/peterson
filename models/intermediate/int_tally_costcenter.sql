WITH stg_costcenter AS (
    SELECT DISTINCT
        ledger_name
        , costcenter_name
        , cost_category
        , {{ literal_name("ledger_name") }} AS literal_ledger_name
        , {{ literal_name("costcenter_name") }} AS literal_costcenter_name
        , {{ literal_name("cost_category") }} AS literal_cost_category
    FROM {{ ref('stg_tally_voucher_costcenter_data') }}
    WHERE customer_name IS NOT NULL
)

, group_services AS (
    SELECT
          cost_category
        , group_services
        , {{ literal_name("cost_category") }} AS literal_cost_category
    FROM {{ ref('tally_group_services') }}
)

, final AS (
    SELECT
        stg_costcenter_data.ledger_name
        , stg_costcenter_data.costcenter_name
        , stg_costcenter_data.cost_category
        , stg_costcenter_data.literal_ledger_name
        , stg_costcenter_data.literal_costcenter_name
        , stg_costcenter_data.literal_cost_category
        , tally_group_services_data.group_services
    FROM stg_costcenter AS stg_costcenter_data
    LEFT JOIN group_services AS tally_group_services_data
      ON stg_costcenter_data.literal_cost_category = tally_group_services_data.literal_cost_category
)

SELECT
    ledger_name
    , costcenter_name
    , cost_category
    , literal_ledger_name
    , literal_costcenter_name
    , literal_cost_category
    , group_services
FROM final