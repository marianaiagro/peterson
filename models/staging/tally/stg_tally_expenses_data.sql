SELECT
    id
    ,trans_id
    ,ledger_name
    ,costcenter_name
    ,costcenter_amount
    ,remote_id
    ,cost_category
    ,comp_name
    ,comp_code
    ,vch_no
    ,ref_no
    ,vch_date
    ,party_name
    ,total_amount
    ,consignee
    ,narration
FROM {{ source('tally', 'get_expensesdata') }} AS expenses_data