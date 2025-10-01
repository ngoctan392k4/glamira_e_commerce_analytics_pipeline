{{ config(
    schema='glamira_analysis',
    alias='dim_stone',
    materialized='table'
) }}

SELECT
    ss.stone_id,
    ss.sku,
    ss.stone_name,
    ss.configure_quality,
    ss.stone_group
FROM {{ref("stg_dim_stone")}} ss
