{{ config(
    materialized='table',
    schema='modeled'
) }}

SELECT 
    organization_id
    ,name
    ,organization_type
    ,status
    ,organization_system
    ,brand
    ,founder
    ,holding
FROM {{ ref('int_customers') }}