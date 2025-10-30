SELECT
      id
    , trans_id
    , NULLIF(TRIM(REGEXP_REPLACE(ledger_name, '\\s+', '')), '')        AS ledger_name
    , NULLIF(TRIM(REGEXP_REPLACE(costcenter_name, '\\s+', '')), '')    AS costcenter_name
    , costcenter_amount
    , remote_id
    , NULLIF(TRIM(REGEXP_REPLACE(cost_category, '\\s+', '')), '')      AS cost_category
    , NULLIF(TRIM(REGEXP_REPLACE(comp_name, '\\s+', '')), '')          AS comp_name
    , NULLIF(TRIM(REGEXP_REPLACE(comp_code, '\\s+', '')), '')          AS comp_code
    , NULLIF(TRIM(REGEXP_REPLACE(vch_no, '\\s+', '')), '')             AS vch_no
    , NULLIF(TRIM(REGEXP_REPLACE(ref_no, '\\s+', '')), '')             AS ref_no
    , vch_date
    , NULLIF(TRIM(REGEXP_REPLACE(party_name, '\\s+', '')), '')         AS party_name
    , total_amount
    , NULLIF(TRIM(REGEXP_REPLACE(consignee, '\\s+', '')), '')          AS consignee
    , NULLIF(TRIM(REGEXP_REPLACE(narration, '\\s+', '')), '')          AS narration
FROM {{ source('tally', 'get_expensesdata') }} AS expenses_data