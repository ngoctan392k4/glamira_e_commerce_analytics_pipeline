{{ config(
    schema='glamira_analysis',
    alias='dim_color',
    materialized='table'
) }}

WITH color_source AS (
  SELECT *
  FROM {{ref("stg_dim_color")}}
)

SELECT *
FROM color_source