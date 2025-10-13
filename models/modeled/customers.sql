WITH icu AS (
    SELECT 
         'icu' AS source_system
        ,client_number
        ,legal_name
        ,legal_number
        ,name
        ,tax_number
        ,website
        ,organization_id
        ,approved
        ,approved_at
        ,organization_type
        ,status
        ,organization_system
        ,brand
        ,founder
        ,holding
        ,NULL AS company_group
        ,icu_literal_name
        ,NULL AS gcis_literal_name
    FROM {{ ref('int_icu_organization') }}
    WHERE organization_type = 'customer' 
       OR (organization_type = 'unit' AND organization_system = 'cert' AND status = 'customer')
),

gcis AS (
    SELECT 
         'gcis' AS source_system
        ,client_number
        ,legal_name
        ,NULL AS legal_number
        ,NULL AS name
        ,tax_number
        ,NULL AS website
        ,NULL AS organization_id
        ,NULL AS approved
        ,NULL AS approved_at
        ,NULL AS organization_type
        ,NULL AS status
        ,NULL AS organization_system
        ,NULL AS brand
        ,NULL AS founder
        ,NULL AS holding
        ,company_group
        ,NULL AS icu_literal_name
        ,gcis_literal_name
    FROM {{ ref('int_gcis_organization') }}
)

SELECT 
    *
FROM icu

UNION ALL

SELECT 
    *
FROM gcis