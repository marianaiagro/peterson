with source_data as
(
    select 
        source_system,
        count(1) as count,
        count(distinct legal_name) as count_legal_name
    from {{ ref('customers') }} as customers
    group by all
)
select *
from source_data