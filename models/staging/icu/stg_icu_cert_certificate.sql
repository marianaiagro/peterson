
WITH source_data AS (
    SELECT
        id
        ,certificate_number
        ,certification_id
        ,document_id
        ,organization_id
        ,status
        ,certifier_user_id
        ,start_date
        ,end_date
        ,contracting_office_id
        ,accredited_office_id
        ,scope_id
        ,validity_start
        ,validity_end
        ,active
        ,created_by
        ,created_date
        ,modified_by
        ,modified_date
        ,mig_bug_fix_program_id
        ,validity_start_offset
        ,validity_end_offset
        ,issued_at
        ,issued_at_offset
        ,deactivation_reason
        ,certification_date
        ,certification_date_offset
        ,dossier_document_id
        ,dossier_id
        ,original_certificate_number
        ,valid_for_tc_by_certifier_end
        ,valid_for_tc_by_certifier_end_offset
        ,valid_for_tc_by_client_end
        ,valid_for_tc_by_client_end_offset
        ,deactivation_comment
        ,deactivation_reason_text
        ,valid
        ,has_document
        ,MAX(_peerdb_synced_at)                 AS _peerdb_synced_at
    FROM {{ source('icu_cert', 'certificate') }} AS certificate
    WHERE _peerdb_is_deleted = FALSE
    GROUP BY
        id
        ,certificate_number
        ,certification_id
        ,document_id
        ,organization_id
        ,status
        ,certifier_user_id
        ,start_date
        ,end_date
        ,contracting_office_id
        ,accredited_office_id
        ,scope_id
        ,validity_start
        ,validity_end
        ,active
        ,created_by
        ,created_date
        ,modified_by
        ,modified_date
        ,mig_bug_fix_program_id
        ,validity_start_offset
        ,validity_end_offset
        ,issued_at
        ,issued_at_offset
        ,deactivation_reason
        ,certification_date
        ,certification_date_offset
        ,dossier_document_id
        ,dossier_id
        ,original_certificate_number
        ,valid_for_tc_by_certifier_end
        ,valid_for_tc_by_certifier_end_offset
        ,valid_for_tc_by_client_end
        ,valid_for_tc_by_client_end_offset
        ,deactivation_comment
        ,deactivation_reason_text
        ,valid
        ,has_document
)

SELECT
    *
FROM source_data