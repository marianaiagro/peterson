
WITH source_data AS (
    SELECT
        id
        ,created_by
        ,created_date
        ,modified_by
        ,modified_date
        ,certified_at
        ,certifier_user_id
        ,expiration_date
        ,is_valid
        ,type
        ,cert_cycle_id
        ,organization_id
        ,active
        ,name
        ,status
        ,MAX(_peerdb_synced_at)                 AS _peerdb_synced_at
    FROM {{ source('icu_cert', 'scope') }} AS scope
    WHERE _peerdb_is_deleted = FALSE
    GROUP BY
        id
        ,created_by
        ,created_date
        ,modified_by
        ,modified_date
        ,certified_at
        ,certifier_user_id
        ,expiration_date
        ,is_valid
        ,type
        ,cert_cycle_id
        ,organization_id
        ,active
        ,name
        ,status
)

SELECT
    *
FROM source_data