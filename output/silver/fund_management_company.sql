{{ config(materialized='table', schema='silver', tags=['SCD4A']) }}

WITH fu_co AS (
    SELECT id, name, shortname, ename, capital, createdby, datecreated, datemodified, naid, statusid, director, depcert, description
    FROM bronze.FUNDCOMPANY
    WHERE data_date = to_date('{{ var("etl_date") }}', 'yyyy-MM-dd')
),

fu_bu_raw AS (
    SELECT fundid, buid
    FROM bronze.FUNDCOMBUSINES
    WHERE data_date = to_date('{{ var("etl_date") }}', 'yyyy-MM-dd')
),

fu_bu AS (
    SELECT fundid, array_agg(buid) AS business_type_codes
    FROM fu_bu_raw
    GROUP BY fundid
),

fu_ty_raw AS (
    SELECT fundid, comtypeid
    FROM bronze.FUNDCOMTYPE
    WHERE data_date = to_date('{{ var("etl_date") }}', 'yyyy-MM-dd')
),

fu_ty AS (
    SELECT fundid, array_agg(comtypeid) AS company_type_codes
    FROM fu_ty_raw
    GROUP BY fundid
)

SELECT
    hash_id('FIMS_FUNDCOMPANY', fu_co.id) AS fund_management_company_id,
    fu_co.id :: string                    AS fund_management_company_code,
    'FIMS_FUNDCOMPANY' :: string          AS source_system_code,
    fu_co.name :: string                  AS fund_management_company_name,
    fu_co.shortname :: string             AS fund_management_company_short_name,
    fu_co.ename :: string                 AS fund_management_company_english_name,
    NULL :: string                        AS practice_status_code,
    fu_co.capital :: decimal(23,2)        AS charter_capital_amount,
    NULL :: string                        AS dorf_indicator,
    NULL :: string                        AS license_decision_number,
    NULL :: date                          AS license_decision_date,
    NULL :: date                          AS active_date,
    NULL :: date                          AS stop_date,
    fu_bu.business_type_codes :: string   AS business_type_codes,
    fu_co.createdby :: string             AS created_by,
    fu_co.datecreated :: timestamp        AS created_timestamp,
    fu_co.datemodified :: timestamp       AS updated_timestamp,
    hash_id('FIMS_NATIONAL', fu_co.naid)  AS country_of_registration_id,
    fu_co.naid :: string                  AS country_of_registration_code,
    fu_co.statusid :: string              AS life_cycle_status_code,
    fu_co.director :: string              AS director_name,
    fu_co.depcert :: string               AS depository_certificate_number,
    fu_ty.company_type_codes :: string    AS company_type_codes,
    fu_co.description :: string           AS description,
    NULL :: string                        AS company_type_code,
    NULL :: string                        AS fund_type_code,
    NULL :: string                        AS business_license_number,
    NULL :: string                        AS website
FROM fu_co
    LEFT JOIN fu_bu ON fu_co.id = fu_bu.fundid
    LEFT JOIN fu_ty ON fu_co.id = fu_ty.fundid
;
