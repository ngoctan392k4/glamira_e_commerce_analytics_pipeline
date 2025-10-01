{{ config(
    schema='glamira_analysis',
    alias='dim_date',
    materialized='table'
) }}

SELECT *
FROM {{ref("stg_dim_date")}}