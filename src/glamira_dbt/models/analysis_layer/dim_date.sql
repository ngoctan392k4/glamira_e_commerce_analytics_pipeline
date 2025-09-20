{{ config(
    schema='glamira_analysis',
    alias='dim_date',
    materialized='table'
) }}

WITH date_source AS (
  SELECT *
  FROM {{ref("stg_dim_date")}}
)

SELECT *
FROM date_source