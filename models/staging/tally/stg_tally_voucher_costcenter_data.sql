SELECT
      NULLIF(TRIM(REGEXP_REPLACE(C1,  '\\s+', '')), '')  AS voucher_id
    , NULLIF(TRIM(REGEXP_REPLACE(C2,  '\\s+', '')), '')  AS remote_id
    , NULLIF(TRIM(REGEXP_REPLACE(C3,  '\\s+', '')), '')  AS company_name
    , NULLIF(TRIM(REGEXP_REPLACE(C4,  '\\s+', '')), '')  AS voucher_type
    , NULLIF(TRIM(REGEXP_REPLACE(C5,  '\\s+', '')), '')  AS voucher_number
    , TO_DATE(TO_TIMESTAMP(C6 / 1000))                   AS voucher_date
    , NULLIF(TRIM(REGEXP_REPLACE(C7,  '\\s+', '')), '')  AS reference_number
    , NULLIF(TRIM(REGEXP_REPLACE(C8,  '\\s+', '')), '')  AS voucher_total_amount
    , NULLIF(TRIM(REGEXP_REPLACE(C9,  '\\s+', '')), '')  AS customer_name
    , NULLIF(TRIM(REGEXP_REPLACE(C10, '\\s+', '')), '')  AS address_line_1
    , NULLIF(TRIM(REGEXP_REPLACE(C11, '\\s+', '')), '')  AS address_line_2
    , NULLIF(TRIM(REGEXP_REPLACE(C12, '\\s+', '')), '')  AS address_line_3
    , NULLIF(TRIM(REGEXP_REPLACE(C13, '\\s+', '')), '')  AS address_line_4
    , NULLIF(TRIM(REGEXP_REPLACE(C14, '\\s+', '')), '')  AS address_line_5
    , NULLIF(TRIM(REGEXP_REPLACE(C15, '\\s+', '')), '')  AS narration
    , NULLIF(TRIM(REGEXP_REPLACE(C16, '\\s+', '')), '')  AS ledger_name
    , NULLIF(TRIM(REGEXP_REPLACE(C17, '\\s+', '')), '')  AS costcenter_name
    , NULLIF(TRIM(REGEXP_REPLACE(C18, '\\s+', '')), '')  AS cost_category
    , NULLIF(TRIM(REGEXP_REPLACE(C19, '\\s+', '')), '')  AS costcenter_amount
    , NULLIF(TRIM(REGEXP_REPLACE(C20, '\\s+', '')), '')  AS id_
FROM {{ source('tally', 'getvouchercostcenterdata') }} AS voucher_costcenter_data
WHERE 
    lower(C1) <> 'id'