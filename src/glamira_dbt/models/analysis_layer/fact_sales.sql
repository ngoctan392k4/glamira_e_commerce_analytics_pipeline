{{ config(
    schema='glamira_analysis',
    alias='fact_sales',
    materialized='table'
) }}

WITH fact_source AS (
    SELECT *
    FROM {{ref("stg_fact_sales")}}
),
stg_location_source AS (
    SELECT *
    FROM {{ref("stg_dim_location")}}
)

SELECT
    fs.sale_id,
    fs.order_id,
    fs.product_id,
    fs.date_id,
    fs.store_id,
    fs.stone_id,
    fs.color_id,
    fs.metal_id,
    fs.customer_id,
    sls.location_id,
    fs.ip_address,
    fs.local_time,
    fs.quantity,
    fs.price,
    CASE
        WHEN exchange_rate_to_usd is not null THEN ROUND(fs.quantity * fs.price * fs.exchange_rate_to_usd, 2)
        ELSE ROUND(fs.quantity*fs.price, 2)
    END AS total_in_usd
FROM fact_source fs
LEFT JOIN stg_location_source sls
    ON fs.ip_address = sls.ip_address