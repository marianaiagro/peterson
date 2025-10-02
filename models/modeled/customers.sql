{{ config(
    materialized='table',
    schema='modeled'
) }}

SELECT 
    *
FROM {{ ref('int_customers') }}