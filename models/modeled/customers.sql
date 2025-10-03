
SELECT 
    client_number
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
FROM {{ ref('int_organization') }}
WHERE organization_type = 'customer'