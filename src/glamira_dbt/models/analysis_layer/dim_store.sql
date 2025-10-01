{{ config(
    schema='glamira_analysis',
    alias='dim_store',
    materialized='table'
) }}


SELECT *
FROM {{ref("stg_dim_store")}}