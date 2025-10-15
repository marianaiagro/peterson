/*************************************************************************/
-- Desc: Programas de certificación para cada cliente por período
-- Fecha: 05/10/2025
-- Modificado por: MRA
/*************************************************************************/

with organization as
(
    select cert_org.id as cert_organization_id,
        org.*
    from {{ ref ('stg_icu_cert_organization') }} as cert_org
        inner join {{ ref ('customers') }} as org
            on cert_org.crm_organization_id = org.organization_id
    where cert_org.dossier_id is not null
)
, scope as
(
    select
        organization.client_number,
        initcap(organization.legal_name) as organization_name,
        organization.country_en as country,
        scope.id as scope_id,
        scope.type as scope_type,
        scope.created_date::date as created_date,
        scope.certified_at::date as certified_date,
        to_varchar(scope.certified_at::date, 'yyyy-mm') as certified_period,
        scope.expiration_date::date as expiration_date,
        scope.is_valid
    from {{ ref ('stg_icu_cert_scope') }} as scope
        left join organization
            on organization.cert_organization_id = scope.organization_id
    where 1=1
    and scope.active
    and created_date > '2025-01-01'
    --and lower(organization.legal_name) like '%cargil%'
    --and client_number = '1079736'
)
, certificates as
(
    select
        scope.*,
        --certificates.created_date,
        --certificates.certification_date,
        certificates.certificate_number,
        replace(split_part(certificates.certificate_number,'-',1), 'CU'||scope.client_number, '') as certification_program,
        --certificates.original_certificate_number,
        --certificates.document_id,
        certificates.status as certificate_status,
        --certificates.contracting_office_id,
        --certificates.accredited_office_id,
        certificates.deactivation_reason,
        certificates.deactivation_comment,
        certificates.deactivation_reason_text
    from {{ ref ('stg_icu_cert_certificate') }} as certificates
        inner join scope
            on certificates.scope_id = scope.scope_id
    where 1 = 1
        and certificates.status != 'draft'
        and certificates.created_date > '2025-01-01'
)
select *
from certificates