WITH source_data AS (

    SELECT
       id
       ,city
       ,country_iso_code
       ,description
       ,is_primary
       ,latitude
       ,line1
       ,line2
       ,line3
       ,longitude
       ,state_iso_code
       ,type
       ,zip
       ,bank_account_id
       ,party_id
       ,_peerdb_is_deleted
       ,_peerdb_synced_at
    FROM {{ source('icu', 'address') }} AS address

)

SELECT
    *
FROM source_data




