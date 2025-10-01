{{ config(
    schema='glamira_analysis',
    alias='dim_metal',
    materialized='table'
) }}

SELECT *
FROM {{ref("stg_dim_metal")}}