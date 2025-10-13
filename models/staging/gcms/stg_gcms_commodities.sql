
WITH source_data AS (

    SELECT
          id
        , name
        , product_category
        , culture_code
        , measure_unit
        , CAST(maximun_validity_term_days AS VARCHAR)     AS maximun_validity_term_days
        , CAST(analysis_required AS VARCHAR)              AS analysis_required
        , CAST(empty AS VARCHAR)                          AS empty
        , CAST(active AS VARCHAR)                         AS active
        , product_variety_name
        , complement
        , CAST(product_variety_active AS VARCHAR)         AS product_variety_active
        , institutions_associated_with_the_product_variety
        , packing_forms_associated_with_the_product
    FROM {{ source('gcms', 'gcms_commodities_emea_apac') }}

    UNION ALL

    SELECT
          id
        , name
        , product_category
        , culture_code
        , measure_unit
        , CAST(maximun_validity_term_days AS VARCHAR)     AS maximun_validity_term_days
        , CAST(analysis_required AS VARCHAR)              AS analysis_required
        , CAST(empty AS VARCHAR)                          AS empty
        , CAST(active AS VARCHAR)                         AS active
        , product_variety_name
        , complement
        , CAST(product_variety_active AS VARCHAR)         AS product_variety_active
        , institutions_associated_with_the_product_variety
        , packing_forms_associated_with_the_product
    FROM {{ source('gcms', 'gcms_commodities_brazil') }}
)

SELECT
      id
    , name
    , product_category
    , culture_code
    , measure_unit
    , maximun_validity_term_days
    , analysis_required
    , empty
    , active
    , product_variety_name
    , complement
    , product_variety_active
    , institutions_associated_with_the_product_variety
    , packing_forms_associated_with_the_product
FROM source_data