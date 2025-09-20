{{ config(
    materialized='table'
) }}

WITH english_product_name AS (
  SELECT
      product_id,
      ARRAY_AGG(rpd ORDER BY min_price LIMIT 1)[OFFSET(0)] AS data
  FROM {{source('glamira_src', 'raw_product_data')}} AS rpd
  WHERE rpd.min_price LIKE '$%'
  GROUP BY product_id
),

other_latin_product_name AS (
  SELECT
      product_id,
      ARRAY_AGG(rpd ORDER BY product_name LIMIT 1)[OFFSET(0)] AS data
  FROM {{source('glamira_src', 'raw_product_data')}} AS rpd
  WHERE rpd.product_id NOT IN (SELECT product_id FROM english_product_name)
    AND REGEXP_CONTAINS(rpd.product_name, r'^[A-Za-z0-9\s\-\.,]+$')
  GROUP BY product_id
),

remaining_products AS (
  SELECT *
  FROM {{source('glamira_src', 'raw_product_data')}} AS rpd
  WHERE rpd.product_id NOT IN (SELECT product_id FROM english_product_name)
    AND rpd.product_id NOT IN (SELECT product_id FROM other_latin_product_name)
),

random_suffix AS (
  SELECT
      product_id,
      ARRAY_AGG(suffix ORDER BY suffix LIMIT 1)[OFFSET(0)] AS picked_suffix
  FROM remaining_products
  GROUP BY product_id
),

remaining_picked AS (
  SELECT rpd.*
  FROM remaining_products rpd
  JOIN random_suffix ps
    ON rpd.product_id = ps.product_id
   AND rpd.suffix = ps.picked_suffix
)


SELECT
  data.product_id,
  COALESCE(NULLIF(data.suffix, ''), 'Not Defined') AS suffix,
  COALESCE(NULLIF(data.product_name, ''), 'Not Defined') AS product_name,
  COALESCE(NULLIF(data.sku, ''), 'Not Defined') AS sku,
  COALESCE(NULLIF(data.attribute_set_id, -1), -1) AS attribute_set_id,
  COALESCE(NULLIF(data.type_id, ''), 'Not Defined') AS type_id,
  COALESCE(NULLIF(data.min_price, ''), 'Not Defined') AS min_price,
  COALESCE(NULLIF(data.max_price, ''), 'Not Defined') AS max_price,
  COALESCE(NULLIF(data.collection_id, ''), 'Not Defined') AS collection_id,
  COALESCE(NULLIF(data.product_type_value, ''), 'Not Defined') AS product_type_value,
  COALESCE(NULLIF(data.category, -1), -1) AS category,
  COALESCE(NULLIF(data.store_code, ''), '-1') AS store_code,
  COALESCE(NULLIF(data.gender, ''), 'Not Defined') AS gender,
  data.stone AS stone,
  data.color AS color,
  data.alloy AS alloy
FROM english_product_name

UNION ALL
SELECT
  data.product_id,
  COALESCE(NULLIF(data.suffix, ''), 'Not Defined') AS suffix,
  COALESCE(NULLIF(data.product_name, ''), 'Not Defined') AS product_name,
  COALESCE(NULLIF(data.sku, ''), 'Not Defined') AS sku,
  COALESCE(NULLIF(data.attribute_set_id, -1), -1) AS attribute_set_id,
  COALESCE(NULLIF(data.type_id, ''), 'Not Defined') AS type_id,
  COALESCE(NULLIF(data.min_price, ''), 'Not Defined') AS min_price,
  COALESCE(NULLIF(data.max_price, ''), 'Not Defined') AS max_price,
  COALESCE(NULLIF(data.collection_id, ''), 'Not Defined') AS collection_id,
  COALESCE(NULLIF(data.product_type_value, ''), 'Not Defined') AS product_type_value,
  COALESCE(NULLIF(data.category, -1), -1) AS category,
  COALESCE(NULLIF(data.store_code, ''), '-1') AS store_code,
  COALESCE(NULLIF(data.gender, ''), 'Not Defined') AS gender,
  data.stone AS stone,
  data.color AS color,
  data.alloy AS alloy
FROM other_latin_product_name

UNION ALL
SELECT
  rp.product_id,
  COALESCE(NULLIF(rp.suffix, ''), 'Not Defined') AS suffix,
  COALESCE(NULLIF(rp.product_name, ''), 'Not Defined') AS product_name,
  COALESCE(NULLIF(rp.sku, ''), 'Not Defined') AS sku,
  COALESCE(NULLIF(rp.attribute_set_id, -1), -1) AS attribute_set_id,
  COALESCE(NULLIF(rp.type_id, ''), 'Not Defined') AS type_id,
  COALESCE(NULLIF(rp.min_price, ''), 'Not Defined') AS min_price,
  COALESCE(NULLIF(rp.max_price, ''), 'Not Defined') AS max_price,
  COALESCE(NULLIF(rp.collection_id, ''), 'Not Defined') AS collection_id,
  COALESCE(NULLIF(rp.product_type_value, ''), 'Not Defined') AS product_type_value,
  COALESCE(NULLIF(rp.category, -1), -1) AS category,
  COALESCE(NULLIF(rp.store_code, ''), '-1') AS store_code,
  COALESCE(NULLIF(NULLIF(rp.gender, 'false'), ''), 'Not Defined') AS gender,
  rp.stone AS stone,
  rp.color AS color,
  rp.alloy AS alloy
FROM remaining_picked rp



