
WITH source_data AS (
    SELECT
        id
        ,client_id
        ,po_number
        ,contact_id
        ,description
        ,issue_date
        ,lang_code
        ,name
        ,number
        ,order_confirmation_number
        ,pdf_doc_store_id
        ,status
        ,total_price
        ,configuration_id
        ,currency_id
        ,offer_version
        ,offer_id
        ,order_confirmation_template_id
        ,template_id
        ,debtor_id
        ,client_accepted
        ,client_accepted_at
        ,created_at
        ,created_by
        ,modified_at
        ,modified_by
        ,printed_at
        ,total_net_amount
        ,comment
        ,MAX(_peerdb_synced_at)                 AS _peerdb_synced_at
    FROM {{ source('icu_pci', 'contract') }} AS contract
    WHERE _peerdb_is_deleted = FALSE
    GROUP BY
        id
        ,client_id
        ,po_number
        ,contact_id
        ,description
        ,issue_date
        ,lang_code
        ,name
        ,number
        ,order_confirmation_number
        ,pdf_doc_store_id
        ,status
        ,total_price
        ,configuration_id
        ,currency_id
        ,offer_version
        ,offer_id
        ,order_confirmation_template_id
        ,template_id
        ,debtor_id
        ,client_accepted
        ,client_accepted_at
        ,created_at
        ,created_by
        ,modified_at
        ,modified_by
        ,printed_at
        ,total_net_amount
        ,comment
)

SELECT
    *
FROM source_data