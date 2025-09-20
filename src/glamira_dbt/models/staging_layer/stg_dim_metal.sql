{{ config(
    materialized='table'
) }}

WITH dim_metal_source AS (
    SELECT *
    FROM {{source('glamira_src', 'raw_glamira_behaviour')}}
),

dim_metal_code_value AS (
    SELECT DISTINCT
        opt.value_label as alloy_value,
        CASE
            WHEN STRPOS(opt.value_label, '&') > 0 THEN SUBSTR(opt.value_label, STRPOS(opt.value_label, '-')+1, STRPOS(opt.value_label, '&') - (STRPOS(opt.value_label, '-')+1))
            ELSE SUBSTR(opt.value_label, STRPOS(opt.value_label, '-')+1)
        END AS metal_code
    FROM dim_metal_source
    JOIN UNNEST(option) AS opt
    WHERE opt.option_label = 'alloy'
),

dim_metal AS (
    SELECT *
    FROM dim_metal_code_value
    WHERE
        metal_code IS NOT NULL
        AND metal_code <> ''
)

SELECT
  DISTINCT
    FARM_FINGERPRINT(metal_code) as metal_id,
    metal_code,
    CASE
        WHEN metal_code= '375' THEN '9K Gold - 375'
        WHEN metal_code= '417' THEN '10K Gold - 417'
        WHEN metal_code= '585' THEN '14K Gold - 585'
        WHEN metal_code= '750' THEN '18K Gold - 750'
        WHEN metal_code= 'platin' THEN '950 Platinum'
        WHEN metal_code= 'platin_375' THEN '375 Platinum'
        WHEN metal_code= 'platin_417' THEN '417 Platinum'
        WHEN metal_code= 'platin_585' THEN '585 Platinum'
        WHEN metal_code= 'platin_750' THEN '750 Platinum'
        WHEN metal_code= 'palladium' THEN '950 Palladium'
        WHEN metal_code= 'silber' THEN '925 Silver'
        WHEN metal_code= 'silber_375' THEN '375 Silver'
        WHEN metal_code= 'silber_417' THEN '417 Silver'
        WHEN metal_code= 'ceramic585' THEN 'Ceramic / 585 Gold'
        WHEN metal_code= 'edelstahl585' THEN 'Edelstahl / 585 Gold'
        WHEN metal_code= 'stainless' THEN 'Stainless Steel'
        ELSE 'Undefined'
    END as metal_name
FROM dim_metal
WHERE metal_code IS NOT NULL

UNION ALL

SELECT
    -1 AS metal_id,
    'Not Defined' AS metal_code,
    'Not Defined' AS metal_name