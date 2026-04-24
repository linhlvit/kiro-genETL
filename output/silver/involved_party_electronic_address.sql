{{ config(materialized='table', schema='silver', tags=['SCD4A']) }}

WITH th_ti_dk_th AS (
    SELECT id, email_kd, fax_kd, fax, dien_thoai_kd, dien_thoai, 'DCST_THONG_TIN_DK_THUE' AS source_system_code
    FROM bronze.THONG_TIN_DK_THUE
    WHERE data_date = to_date('{{ var("etl_date") }}', 'yyyy-MM-dd')
),

leg_th_ti_dk_th AS (
    SELECT
        id AS ip_code,
        'EMAIL' AS type_code,
        email_kd AS address_value,
        source_system_code
    FROM th_ti_dk_th
    WHERE email_kd IS NOT NULL
    UNION ALL
    SELECT
        id AS ip_code,
        'FAX' AS type_code,
        fax_kd AS address_value,
        source_system_code
    FROM th_ti_dk_th
    WHERE fax_kd IS NOT NULL
    UNION ALL
    SELECT
        id AS ip_code,
        'FAX' AS type_code,
        fax AS address_value,
        source_system_code
    FROM th_ti_dk_th
    WHERE fax IS NOT NULL
    UNION ALL
    SELECT
        id AS ip_code,
        'PHONE' AS type_code,
        dien_thoai_kd AS address_value,
        source_system_code
    FROM th_ti_dk_th
    WHERE dien_thoai_kd IS NOT NULL
    UNION ALL
    SELECT
        id AS ip_code,
        'PHONE' AS type_code,
        dien_thoai AS address_value,
        source_system_code
    FROM th_ti_dk_th
    WHERE dien_thoai IS NOT NULL
),

tt_ng_da_di AS (
    SELECT id, email, fax, dien_thoai, 'DCST_TTKDT_NGUOI_DAI_DIEN' AS source_system_code
    FROM bronze.TTKDT_NGUOI_DAI_DIEN
    WHERE data_date = to_date('{{ var("etl_date") }}', 'yyyy-MM-dd')
),

leg_tt_ng_da_di AS (
    SELECT
        id AS ip_code,
        'EMAIL' AS type_code,
        email AS address_value,
        source_system_code
    FROM tt_ng_da_di
    WHERE email IS NOT NULL
    UNION ALL
    SELECT
        id AS ip_code,
        'FAX' AS type_code,
        fax AS address_value,
        source_system_code
    FROM tt_ng_da_di
    WHERE fax IS NOT NULL
    UNION ALL
    SELECT
        id AS ip_code,
        'PHONE' AS type_code,
        dien_thoai AS address_value,
        source_system_code
    FROM tt_ng_da_di
    WHERE dien_thoai IS NOT NULL
)

SELECT
    hash_id(leg_th_ti_dk_th.source_system_code, leg_th_ti_dk_th.ip_code) AS involved_party_id,
    leg_th_ti_dk_th.ip_code :: string                                    AS involved_party_code,
    leg_th_ti_dk_th.source_system_code :: string                         AS source_system_code,
    leg_th_ti_dk_th.type_code :: string                                  AS electronic_address_type_code,
    leg_th_ti_dk_th.address_value :: string                              AS electronic_address_value
FROM th_ti_dk_th
SELECT * FROM leg_th_ti_dk_th
UNION ALL
SELECT * FROM leg_tt_ng_da_di
;
