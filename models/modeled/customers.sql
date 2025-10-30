WITH icu AS (
    SELECT 
         'icu'                          AS source_system
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
        ,NULL                           AS company_group
        ,NULL                           AS economic_group
        ,NULL                           AS parent_company
        ,icu_literal_name               AS literal_name
        ,word_tags
        ,country_en
        ,country_code
        ,icu_tax_number_literal         AS literal_tax_number
    FROM {{ ref('int_icu_organization') }}
    WHERE organization_type = 'customer' 
       OR (organization_type = 'unit' AND organization_system = 'cert') -- AND status = 'customer')
)

, gcis AS (
    SELECT 
         'gcis'                         AS source_system
        ,client_number
        ,legal_name
        ,NULL                           AS legal_number
        ,NULL                           AS name
        ,tax_number
        ,NULL                           AS website
        ,NULL                           AS organization_id
        ,NULL                           AS approved
        ,NULL                           AS approved_at
        ,NULL                           AS organization_type
        ,NULL                           AS status
        ,NULL                           AS organization_system
        ,NULL                           AS brand
        ,NULL                           AS founder
        ,NULL                           AS holding
        ,company_group
        ,NULL                           AS economic_group
        ,NULL                           AS parent_company
        ,gcis_literal_name              AS literal_name
        ,word_tags
        ,country_en
        ,country_code
        ,gcis_tax_number_literal        AS literal_tax_number
    FROM {{ ref('int_gcis_organization') }}
)

, gcms AS (
    SELECT
         'gcms'                         AS source_system
        ,client_number          
        ,legal_name         
        ,NULL                           AS legal_number
        ,name           
        ,NULL                           AS tax_number
        ,NULL                           AS website
        ,NULL                           AS organization_id
        ,NULL                           AS approved
        ,NULL                           AS approved_at
        ,NULL                           AS organization_type
        ,NULL                           AS status
        ,NULL                           AS organization_system
        ,NULL                           AS brand
        ,NULL                           AS founder
        ,NULL                           AS holding
        ,NULL                           AS company_group
        ,economic_group         
        ,parent_company         
        ,gcms_literal_name              AS literal_name
        ,word_tags
        ,country_en
        ,country_code
        ,NULL                           AS literal_tax_number
    FROM {{ ref('int_gcms_organization') }}
)



, tally AS (
    SELECT
          'tally'                        AS source_system
        , NULL                           AS client_number
        , legal_name
        , NULL                           AS legal_number
        , NULL                           AS name
        , NULL                           AS tax_number
        , NULL                           AS website
        , NULL                           AS organization_id
        , NULL                           AS approved
        , NULL                           AS approved_at
        , NULL                           AS organization_type
        , NULL                           AS status
        , NULL                           AS organization_system
        , NULL                           AS brand
        , NULL                           AS founder
        , NULL                           AS holding
        , NULL                           AS company_group
        , NULL                           AS economic_group
        , NULL                           AS parent_company
        , tally_literal_name             AS literal_name
        , word_tags
        , country_en
        , country_code
        , NULL                           AS literal_tax_number
    FROM {{ ref('int_tally_organization') }}
)

SELECT * FROM icu
UNION ALL
SELECT * FROM gcis
UNION ALL
SELECT * FROM gcms
UNION ALL
SELECT * FROM tally