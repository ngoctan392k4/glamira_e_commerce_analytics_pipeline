{{ config(
    schema='glamira_analysis',
    alias='dim_product',
    materialized='table'
) }}

WITH product_source AS (
  SELECT *
  FROM {{ref("stg_dim_product")}}
)


SELECT
  ps.product_id,
  ps.suffix,
  ps.product_name,
  ps.sku,
  ps.attribute_set_id,
  ps.type_id,
  ps.min_price,
  ps.max_price,
  ps.collection_id,
  ps.product_type_value,
  ps.category as product_subtype_id,
  ps.store_code,
  ps.gender
FROM product_source ps

UNION ALL

SELECT
  '-1' AS product_id,
  'Not Defined' AS suffix,
  'Not Defined' AS product_name,
  'Not Defined' AS sku,
  -1 AS attribute_set_id,
  'Not Defined' AS type_id,
  'Not Defined' AS min_price,
  'Not Defined' AS max_price,
  'Not Defined' AS collection_id,
  'Not Defined' AS product_type_value,
  -1 AS category,
  '-1' AS store_code,
  'Not Defined' AS gender

-- FROM english_product_name

-- UNION ALL
-- SELECT data.product_id, data.product_name, data.sku, data.attribute_set_id, data.type_id, data.min_price, data.max_price, data.collection_id, data.product_type_value, data.category as product_subtype_id, data.store_code, data.gender
-- FROM other_latin_product_name

-- UNION ALL
-- SELECT data.product_id, data.product_name, data.sku, data.attribute_set_id, data.type_id, data.min_price, data.max_price, data.collection_id, data.product_type_value, data.category as product_subtype_id, data.store_code, data.gender
-- FROM remaining_picked as data
