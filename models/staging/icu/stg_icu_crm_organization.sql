/*
------------------------------------------------------------------------------
Table Name        : stg_icu__organization
Owner             : iAgro
Created On        : 2025-10-01
Brief Description : 
------------------------------------------------------------------------------
*/

WITH source_data AS (
    SELECT
        client_number
      , invoice_format
      , legal_name
      , legal_number
      , name
      , tax_number
      , type
      , website
      , id
      , status
      , deleted
      , time_zone_id
      , approved
      , deleted_at
      , deleted_by
      , approval_priority
      , duplicated
      , duplicated_at
      , duplicated_by
      , approved_at
      , approved_by
      , number_of_employees
      , MAX(_peerdb_synced_at) AS _peerdb_synced_at
    FROM {{ source('icu_crm', 'organization') }} AS organization
    WHERE _peerdb_is_deleted = FALSE
    GROUP BY
        client_number, invoice_format, legal_name, legal_number, name, tax_number
      , type, website, id, status, deleted, time_zone_id, approved, deleted_at
      , deleted_by, approval_priority, duplicated, duplicated_at, duplicated_by
      , approved_at, approved_by, number_of_employees, _peerdb_is_deleted
)

SELECT
      CAST(client_number                        AS VARCHAR)  AS client_number
    , CAST(invoice_format                       AS VARCHAR)  AS invoice_format
    , CAST(legal_name                           AS VARCHAR)  AS legal_name
    , CAST(legal_number                         AS VARCHAR)  AS legal_number
    , CAST(name                                 AS VARCHAR)  AS name
    , CAST(tax_number                           AS VARCHAR)  AS tax_number
    , CAST(type                                 AS VARCHAR)  AS type
    , CAST(website                              AS VARCHAR)  AS website
    , CAST(id                                   AS VARCHAR)  AS id
    , CAST(status                               AS VARCHAR)  AS status
    , deleted                                                AS deleted
    , CAST(time_zone_id                         AS VARCHAR)  AS time_zone_id
    , TRY_TO_BOOLEAN(TO_VARCHAR(approved))                   AS approved
    , approved_at                                            AS approved_at
    , deleted_at                                             AS deleted_at
    , CAST(deleted_by                           AS VARCHAR)  AS deleted_by
    , CAST(approval_priority                    AS VARCHAR)  AS approval_priority
    , TRY_TO_BOOLEAN(TO_VARCHAR(duplicated))                 AS duplicated
    , duplicated_at                                          AS duplicated_at
    , CAST(duplicated_by                        AS VARCHAR)  AS duplicated_by
    , CAST(approved_by                          AS VARCHAR)  AS approved_by
    , TRY_TO_NUMBER(number_of_employees)                     AS number_of_employees
    , _peerdb_synced_at                                      AS _peerdb_synced_at
FROM source_data
