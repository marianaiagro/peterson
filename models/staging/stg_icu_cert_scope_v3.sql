
WITH source_data AS (
    SELECT
        id
        ,cert_cycle_id
        ,organization_id
        ,snapshot_id
        ,MAX(_peerdb_synced_at)                 AS _peerdb_synced_at
    FROM {{ source('icu_cert', 'scope_v3') }} AS scope_v3
    WHERE _peerdb_is_deleted = FALSE
    GROUP BY
        id
        ,cert_cycle_id
        ,organization_id
        ,snapshot_id
)

SELECT
    *
FROM source_data