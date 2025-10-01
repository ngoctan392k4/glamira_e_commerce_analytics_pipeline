{{ config(
    schema='glamira_mart',
    materialized='table'
) }}

WITH product_performance AS(
    SELECT DISTINCT
        p.product_id,
        p.product_name,
        p.sku,
        c.color_name,
        m.metal_name,
        st.stone_name,
        SUM(fs.quantity) AS total_quantity,
        SUM(fs.total_in_usd) AS total_revenue
    FROM {{ref("fact_sales")}} fs
    LEFT JOIN {{ ref("dim_product") }} p
           ON fs.product_id = p.product_id
    LEFT JOIN {{ ref("dim_color") }} c
           ON fs.color_id = c.color_id
    LEFT JOIN {{ ref("dim_metal") }} m
           ON fs.metal_id = m.metal_id
    LEFT JOIN {{ ref("dim_stone") }} st
           ON fs.stone_id = st.stone_id
    GROUP BY 1,2,3,4,5,6
)

SELECT *
FROM product_performance