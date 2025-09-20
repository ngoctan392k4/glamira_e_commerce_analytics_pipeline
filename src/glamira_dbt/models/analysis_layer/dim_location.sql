{{ config(
    schema='glamira_analysis',
    alias='dim_location',
    materialized='table'
) }}

WITH location_source AS (
    SELECT *
    FROM {{ref("stg_dim_location")}}
)

SELECT DISTINCT
    ls.location_id,
    ls.country_name,
    ls.country_short,
    ls.region_name,
    ls.city_name
FROM location_source ls