{{ config(
    schema='glamira_analysis',
    alias='dim_metal',
    materialized='table'
) }}

WITH metal_source AS (
  SELECT *
  FROM {{ref("stg_dim_metal")}}
)

SELECT *
FROM metal_source