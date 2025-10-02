
WITH source_data AS (
    SELECT
        raw
    FROM {{ source('gcis','gcis_companies') }}
)

SELECT
    *
FROM source_data
