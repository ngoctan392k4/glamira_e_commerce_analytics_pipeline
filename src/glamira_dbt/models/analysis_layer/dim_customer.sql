{{ config(
    schema='glamira_analysis',
    alias='dim_customer',
    materialized='table'
) }}

WITH customer_source AS (
  SELECT *
  FROM {{ref("stg_dim_customer")}}
)

SELECT *
FROM customer_source