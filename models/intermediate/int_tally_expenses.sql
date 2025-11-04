
SELECT DISTINCT
    customer_name
    , costcenter_name
    , id
    , voucher_type
    , voucher_number
    , voucher_date
    , try_to_number(costcenter_amount)    AS costcenter_amount
    , try_to_number(voucher_total_amount) AS voucher_total_amount
    , {{ literal_name("costcenter_name") }} AS literal_costcenter_name
FROM {{ ref('stg_tally_voucher_costcenter_data') }}
WHERE customer_name IS NOT NULL