{{ config(
    materialized='view',
    schema='intermediate'
) }}

WITH stg_icu_organization AS (
    SELECT
        id
        ,name
    FROM {{ ref('stg_icu_organization') }}
),

stg_icu_organization_tag AS (
    SELECT 
        id
        ,name
        ,organization_id
    FROM {{ ref('stg_icu_organization_tag') }}
),

int_organization_tag AS (
    SELECT 
        organization_id
        ,organization_type
        ,status
        ,organization_system
        ,brand
        ,founder
        ,holding
    FROM {{ ref('int_organization_tag') }}
)
SELECT
    stg_icu_organization_tag.organization_id
    ,stg_icu_organization.name AS organization_name
    ,int_organization_tag.organization_type
    ,int_organization_tag.status
    ,int_organization_tag.organization_system
    ,int_organization_tag.brand
    ,int_organization_tag.founder
    ,int_organization_tag.holding
FROM stg_icu_organization_tag
INNER JOIN int_organization_tag
    ON stg_icu_organization_tag.organization_id = int_organization_tag.organization_id
INNER JOIN stg_icu_organization
    ON stg_icu_organization_tag.organization_id = stg_icu_organization.id
WHERE int_organization_tag.organization_type = 'customer'
