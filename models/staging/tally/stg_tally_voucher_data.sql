SELECT
      id
    , remote_id
    , NULLIF(TRIM(REGEXP_REPLACE(company_name, '\\s+', '')), '')        AS company_name
    , NULLIF(TRIM(REGEXP_REPLACE(voucher_type, '\\s+', '')), '')        AS voucher_type
    , NULLIF(TRIM(REGEXP_REPLACE(voucher_number, '\\s+', '')), '')      AS voucher_number
    , voucher_date
    , NULLIF(TRIM(REGEXP_REPLACE(reference_number, '\\s+', '')), '')    AS reference_number
    , voucher_total_amount
    , NULLIF(TRIM(REGEXP_REPLACE(customer_name, '\\s+', '')), '')       AS customer_name
    , NULLIF(TRIM(REGEXP_REPLACE(address_line_1, '\\s+', '')), '')      AS address_line_1
    , NULLIF(TRIM(REGEXP_REPLACE(address_line_2, '\\s+', '')), '')      AS address_line_2
    , NULLIF(TRIM(REGEXP_REPLACE(address_line_3, '\\s+', '')), '')      AS address_line_3
    , NULLIF(TRIM(REGEXP_REPLACE(address_line_4, '\\s+', '')), '')      AS address_line_4
    , NULLIF(TRIM(REGEXP_REPLACE(address_line_5, '\\s+', '')), '')      AS address_line_5
    , NULLIF(TRIM(REGEXP_REPLACE(narration, '\\s+', '')), '')           AS narration
    , sr_no
    , NULLIF(TRIM(REGEXP_REPLACE(type, '\\s+', '')), '')                AS type
    , NULLIF(TRIM(REGEXP_REPLACE(ledger_name, '\\s+', '')), '')         AS ledger_name
    , per
    , amount
    , id_
FROM {{ source('tally', 'get_voucherdata') }} AS voucher_data