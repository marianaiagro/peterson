
SELECT
    stg_icu_organization.client_number          AS client_number
    ,stg_icu_organization.legal_name            AS legal_name
    ,stg_icu_organization.legal_number          AS legal_number
    ,stg_icu_organization.name                  AS name
    ,stg_icu_organization.tax_number            AS tax_number
    ,stg_icu_organization.website               AS website
    ,stg_icu_organization.id                    AS organization_id
    ,stg_icu_organization.approved              AS approved
    ,stg_icu_organization.approved_at           AS approved_at
    ,int_organization_tag.organization_type     AS organization_type
    ,int_organization_tag.status                AS status
    ,int_organization_tag.organization_system   AS organization_system
    ,int_organization_tag.brand                 AS brand
    ,int_organization_tag.founder               AS founder
    ,int_organization_tag.holding               As holding
FROM {{ ref('stg_icu_crm_organization') }} AS stg_icu_organization
INNER JOIN {{ ref('int_organization_tag') }} AS int_organization_tag
    ON stg_icu_organization.id = int_organization_tag.organization_id
