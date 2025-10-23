with address_src as (

    select
          cast(stg_gcis_companies.company_id as varchar)                                 as client_number
        , stg_gcis_companies.address_street                                              as address_street_original
        , {{ word_tags("stg_gcis_companies.address_street") }}                           as address_street_word_tags
        , stg_gcis_companies.address_number                                              as address_number_original
        , {{ word_tags("stg_gcis_companies.address_number") }}                           as address_number_word_tags
        , stg_gcis_companies.address_city                                                as address_city_original
        , {{ word_tags("stg_gcis_companies.address_city") }}                             as address_city_word_tags
        , stg_gcis_companies.address_state                                               as address_state_original
        , {{ word_tags("stg_gcis_companies.address_state") }}                            as address_state_word_tags
        , stg_gcis_companies.address_country                                             as address_country_original
        , {{ word_tags("stg_gcis_companies.address_country") }}                          as address_country_word_tags
        , stg_gcis_companies.address_zipcode                                             as address_zipcode_original
        , {{ word_tags("stg_gcis_companies.address_zipcode") }}                          as address_zipcode_word_tags
    from {{ ref('stg_gcis_companies') }}

), zip_code_src as (

    select
          country_code
        , postal_code
        , place_name
        , {{ word_tags("place_name") }}                                                  as place_name_word_tags
        , admin_name1
        , admin_code1
        , admin_name2
        , admin_code2
        , admin_name3
        , admin_code3
        , accuracy
    from {{ ref('stg_adhoc_country_zip_code') }}

), country_seed as (

    select
          code                                                                            as country_code
        , country_en
    from {{ ref('country') }}

)

select
      address_src.client_number
    , address_src.address_city_word_tags
    , address_src.address_zipcode_word_tags
    , upper(zip_code_src.country_code)                                                   as country_code
    , country_seed.country_en                                                            as country_en
    , zip_code_src.postal_code                                                           as adhoc_postal_code
    , zip_code_src.place_name                                                            as adhoc_city
    , zip_code_src.place_name_word_tags                                                  as adhoc_city_word_tags
from address_src
inner join zip_code_src
    on trim(address_src.address_zipcode_word_tags) = trim(zip_code_src.postal_code)
   and trim(address_src.address_city_word_tags)     = trim(zip_code_src.place_name_word_tags)
left join country_seed
    on upper(zip_code_src.country_code) = upper(country_seed.country_code)