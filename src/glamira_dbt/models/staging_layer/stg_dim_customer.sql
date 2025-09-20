{{ config(
    materialized='table'
) }}

WITH dim_customer_source AS (
    SELECT *
    FROM {{source('glamira_src', 'raw_glamira_behaviour')}}
)

SELECT DISTINCT
    COALESCE(dcs.device_id, '-1') AS customer_id,
    COALESCE(NULLIF(dcs.email_address, ''), 'Not Defined') AS email_address,
    COALESCE(NULLIF(dcs.user_agent, ''), 'Not Defined') AS user_agent,
    COALESCE(NULLIF(dcs.user_id_db, ''), 'Not Defined') AS user_id_db,
    COALESCE(NULLIF(dcs.resolution, ''), 'Not Defined') AS resolution,
    COALESCE(NULLIF(dcs.utm_source, ''), 'Not Defined') AS utm_source,
    COALESCE(NULLIF(dcs.utm_medium, ''), 'Not Defined') AS utm_medium
FROM dim_customer_source dcs

UNION ALL

SELECT
    '-1' AS customer_id,
    'Not Defined' AS email_address,
    'Not Defined' AS user_agent,
    'Not Defined' AS user_id_db,
    'Not Defined' AS resolution,
    'Not Defined' AS utm_source,
    'Not Defined' AS utm_medium