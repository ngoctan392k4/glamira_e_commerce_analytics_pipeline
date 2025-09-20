{{ config(
    materialized='table'
) }}

WITH dim_location_source AS (
    SELECT *
    FROM {{source('glamira_src', 'raw_ip_location')}}
)

SELECT DISTINCT
    FARM_FINGERPRINT(dls.country || dls.region || dls.city) AS location_id,
    dls.ip AS ip_address,
    COALESCE(NULLIF(dls.country, '-'), 'Not Defined') AS country_name,
    COALESCE(NULLIF(dls.country_short, '-'), 'Not Defined') AS country_short,
    COALESCE(NULLIF(dls.region, '-'), 'Not Defined') AS region_name,
    COALESCE(NULLIF(dls.city, '-'), 'Not Defined') AS city_name
FROM dim_location_source dls

UNION ALL

SELECT
    -1 AS location_id,
    'Not Defined' AS ip_address,
    'Not Defined' AS country_name,
    'Not Defined' AS country_short,
    'Not Defined' AS region_name,
    'Not Defined' AS city_name