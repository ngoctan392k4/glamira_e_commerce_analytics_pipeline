{{ config(
    schema='glamira_mart',
    materialized='table'
) }}

WITH revenue_analysis AS (
    SELECT
        fs.sale_id,
        fs.order_id,
        fs.customer_id,
        fs.date_id,
        d.full_date,
        d.year_month,
        s.store_name,
        fs.total_in_usd AS revenue,
        fs.quantity,
        l.country_name
    FROM {{ref("fact_sales")}} fs
    LEFT JOIN {{ref("dim_date")}} d ON fs.date_id = d.date_id
    LEFT JOIN {{ref("dim_store")}} s ON fs.store_id = s.store_id
    LEFT JOIN {{ref("dim_location")}} l ON fs.location_id = l.location_id
)

SELECT *
FROM revenue_analysis