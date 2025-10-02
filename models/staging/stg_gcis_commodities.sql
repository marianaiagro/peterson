
WITH source_data AS (
    SELECT
        raw
    FROM {{ source('gcis','gcis_commodities') }}
)

SELECT
    *
FROM source_data
