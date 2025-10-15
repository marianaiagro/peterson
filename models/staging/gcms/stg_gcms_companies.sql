WITH source_data AS (

    SELECT
          id
        , person_type
        , economic_group
        , name
        , trade_name
        , founded_date
        , parent_company
        , country
        , state
        , city
        , street
        , street_number
        , address_complement
        , postal_code
        , neighborhood
        , complement_type
        , complement_value
        , contact_name
        , contact_type
        , phone_1
        , phone_2
        , email
        , role
        , authorized_service
        , upper('latam')            AS source_companies 
    FROM {{ source('gcms', 'gcms_companies_latam') }}

    UNION ALL

    SELECT
          id
        , person_type
        , economic_group
        , name
        , trade_name
        , founded_date
        , parent_company
        , country
        , state
        , city
        , street
        , street_number
        , address_complement
        , postal_code
        , neighborhood
        , complement_type
        , complement_value
        , contact_name
        , contact_type
        , phone_1
        , phone_2
        , email
        , role
        , authorized_service
        , upper('brazil')            AS source_companies 
    FROM {{ source('gcms', 'gcms_companies_brazil') }}

),

seed_country AS (
    SELECT
          uuid
        , code
        , country_en
        , country_es
    FROM {{ ref('country') }}
),

final AS (
    SELECT
        CAST(source_data.id AS VARCHAR)                          AS client_number
        , source_data.person_type
        , source_data.economic_group
        , source_data.name
        , source_data.trade_name
        , source_data.founded_date
        , source_data.parent_company
        , COALESCE(seed_country.country_en, source_data.country) AS country
        , seed_country.code AS country_code
        , source_data.state
        , source_data.city
        , source_data.street
        , source_data.street_number
        , source_data.address_complement
        , source_data.postal_code
        , source_data.neighborhood
        , source_data.complement_type
        , source_data.complement_value
        , source_data.contact_name
        , source_data.contact_type
        , source_data.phone_1
        , source_data.phone_2
        , source_data.email
        , source_data.role
        , source_data.authorized_service
        , source_data.source_companies 
    FROM source_data
    LEFT JOIN seed_country
        ON LOWER(TRIM(source_data.country)) = LOWER(TRIM(seed_country.country_en))
        OR LOWER(TRIM(source_data.country)) = LOWER(TRIM(seed_country.country_es))
)

SELECT *
FROM final