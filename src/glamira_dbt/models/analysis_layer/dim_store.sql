{{ config(
    schema='glamira_analysis',
    alias='dim_store',
    materialized='table'
) }}

WITH store_source AS (
  SELECT *
  FROM {{ref("stg_dim_store")}}
)

SELECT *
FROM store_source