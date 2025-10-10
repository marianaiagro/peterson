WITH icu AS (
    SELECT 
         'icu'::VARCHAR                           AS source_system
        ,CAST(client_number AS VARCHAR)           AS client_number
        ,CAST(legal_name AS VARCHAR)              AS legal_name
        ,CAST(legal_number AS VARCHAR)            AS legal_number
        ,CAST(name AS VARCHAR)                    AS name
        ,CAST(tax_number AS VARCHAR)              AS tax_number
        ,CAST(website AS VARCHAR)                 AS website
        ,CAST(organization_id AS VARCHAR)         AS organization_id
        ,CAST(approved AS BOOLEAN)                AS approved
        ,CAST(approved_at AS TIMESTAMP)           AS approved_at
        ,CAST(organization_type AS VARCHAR)       AS organization_type
        ,CAST(status AS VARCHAR)                  AS status
        ,CAST(organization_system AS VARCHAR)     AS organization_system
        ,CAST(brand AS VARCHAR)                   AS brand
        ,CAST(founder AS VARCHAR)                 AS founder
        ,CAST(holding AS VARCHAR)                 AS holding
        ,NULL::VARCHAR                            AS company_group
        ,CAST(icu_literal_name AS VARCHAR)        AS icu_literal_name
        ,NULL::VARCHAR                            AS gcis_literal_name
    FROM {{ ref('int_icu_organization') }}
    WHERE organization_type = 'customer' 
       OR (organization_type = 'unit' AND organization_system = 'cert' AND status = 'customer')
)
,gcis AS (
    SELECT 
         'gcis'::VARCHAR                          AS source_system
        ,CAST(client_number AS VARCHAR)           AS client_number
        ,CAST(legal_name AS VARCHAR)              AS legal_name
        ,NULL::VARCHAR                            AS legal_number
        ,NULL::VARCHAR                            AS name
        ,CAST(tax_number AS VARCHAR)              AS tax_number
        ,NULL::VARCHAR                            AS website
        ,NULL::VARCHAR                            AS organization_id
        ,NULL::BOOLEAN                            AS approved
        ,NULL::TIMESTAMP                          AS approved_at
        ,NULL::VARCHAR                            AS organization_type
        ,NULL::VARCHAR                            AS status
        ,NULL::VARCHAR                            AS organization_system
        ,NULL::VARCHAR                            AS brand
        ,NULL::VARCHAR                            AS founder
        ,NULL::VARCHAR                            AS holding
        ,CAST(company_group AS VARCHAR)           AS company_group
        ,NULL::VARCHAR                            AS icu_literal_name
        ,CAST(gcis_literal_name AS VARCHAR)       AS gcis_literal_name
    FROM {{ ref('int_gcis_organization') }}
)

SELECT 
     *
FROM icu

UNION ALL

SELECT 
     *
FROM gcis