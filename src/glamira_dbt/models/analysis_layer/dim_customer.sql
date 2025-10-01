{{ config(
    schema='glamira_analysis',
    alias='dim_customer',
    materialized='table'
) }}

SELECT *
FROM {{ref("stg_dim_customer")}}