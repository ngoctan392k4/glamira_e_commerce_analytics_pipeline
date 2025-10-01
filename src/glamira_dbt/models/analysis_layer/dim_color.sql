{{ config(
    schema='glamira_analysis',
    alias='dim_color',
    materialized='table'
) }}

SELECT *
FROM {{ref("stg_dim_color")}}