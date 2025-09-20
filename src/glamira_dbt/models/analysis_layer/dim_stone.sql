{{ config(
    schema='glamira_analysis',
    alias='dim_stone',
    materialized='table'
) }}

WITH stone_source AS (
    SELECT *
    FROM {{ref("stg_dim_stone")}}
)

SELECT
    ss.stone_id,
    ss.option_id AS stone_type_id,
    ss.option_type_id AS stone_name_id,
    ss.sku,
    ss.default_title AS title,
    ss.configure_quality,
    ss.stone_group
FROM stone_source ss
