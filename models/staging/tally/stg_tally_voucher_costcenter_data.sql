SELECT
    C1   AS id
    ,C2  AS remote_id
    ,C3  AS company_name
    ,C4  AS voucher_type
    ,C5  AS voucher_number
    ,C6  AS voucher_date
    ,C7  AS reference_number
    ,C8  AS voucher_total_amount
    ,C9  AS customer_name
    ,C10 AS address_line_1
    ,C11 AS address_line_2
    ,C12 AS address_line_3
    ,C13 AS address_line_4
    ,C14 AS address_line_5
    ,C15 AS narration
    ,C16 AS ledger_name
    ,C17 AS costcenter_name
    ,C18 AS cost_category
    ,C19 AS costcenter_amount
    ,C20 AS id_
FROM {{ source('tally', 'getvouchercostcenterdata') }} AS voucher_costcenter_data
WHERE 
    lower(C1) <> 'id'
