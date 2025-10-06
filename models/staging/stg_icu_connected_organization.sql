
WITH source_data AS (
    SELECT
        id
        ,created_by
        ,created_date
        ,modified_by
        ,modified_date
        ,crm_organization_id
        ,is_originator
        ,status
        ,type
        ,MAX(_peerdb_synced_at)                 AS _peerdb_synced_at
    FROM {{ source('icu_connected', 'organization') }} AS organization
    WHERE _peerdb_is_deleted = FALSE
    GROUP BY
     id
    ,created_by
    ,created_date
    ,modified_by
    ,modified_date
    ,crm_organization_id
    ,is_originator
    ,status
    ,type
)

SELECT
    *
FROM source_data