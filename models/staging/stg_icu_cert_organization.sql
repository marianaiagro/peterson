
WITH source_data AS (
    SELECT
        id
        ,crm_organization_id
        ,type
        ,dossier_id
        ,has_material_percentage
        ,MAX(_peerdb_synced_at)                 AS _peerdb_synced_at
    FROM {{ source('icu_cert', 'organization') }} AS organization
    WHERE _peerdb_is_deleted = FALSE
    GROUP BY
        id
        ,crm_organization_id
        ,type
        ,dossier_id
        ,has_material_percentage
)

SELECT
    *
FROM source_data