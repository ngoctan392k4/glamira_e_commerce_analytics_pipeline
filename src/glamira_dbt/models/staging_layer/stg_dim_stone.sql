{{ config(
    materialized='table'
) }}

WITH dim_stone_source AS (
    SELECT *
    FROM {{ ref("stg_dim_product") }}
),

stone AS (
    SELECT
        st.*
    FROM dim_stone_source dsc
    CROSS JOIN UNNEST(dsc.stone) AS st
),

data_stone AS (
    SELECT
        FARM_FINGERPRINT(st.option_id || '-' || st.option_type_id) AS stone_id,
        st.option_id,
        st.option_type_id,
        st.sku,
        st.default_title,
        st.configure_quality,
        st.stone_group,

        -- ARRAY_CONCAT_AGG(ARRAY(SELECT t.default_label FROM UNNEST(s.stone_type) t WHERE t.default_label IS NOT NULL)) AS stone_type_default_label,
        -- ARRAY_CONCAT_AGG(ARRAY(SELECT n.default_label FROM UNNEST(s.stone_name) n WHERE n.default_label IS NOT NULL)) AS stone_name_default_label,
        -- ARRAY_CONCAT_AGG(ARRAY(SELECT c.default_label FROM UNNEST(s.certificate) c WHERE c.default_label IS NOT NULL)) AS stone_certificate_default_label,
        -- ARRAY_CONCAT_AGG(ARRAY(SELECT ca.default_label FROM UNNEST(s.carat) ca WHERE ca.default_label IS NOT NULL)) AS carat_default_label,
        -- ARRAY_CONCAT_AGG(ARRAY(SELECT tc.default_label FROM UNNEST(s.total_carat) tc WHERE tc.default_label IS NOT NULL)) AS total_carat_default_label,
        -- ARRAY_CONCAT_AGG(ARRAY(SELECT d.default_label FROM UNNEST(s.diameter) d WHERE d.default_label IS NOT NULL)) AS diameter_default_label,
        -- ARRAY_CONCAT_AGG(ARRAY(SELECT sh.default_label FROM UNNEST(s.shape) sh WHERE sh.default_label IS NOT NULL)) AS shape_default_label,
        -- ARRAY_CONCAT_AGG(ARRAY(SELECT cl.default_label FROM UNNEST(s.clarity) cl WHERE cl.default_label IS NOT NULL)) AS clarity_default_label

    FROM stone st
    -- CROSS JOIN UNNEST(st.data_stones) AS s
    -- GROUP BY FARM_FINGERPRINT(st.option_id || '-' || st.option_type_id), st.option_id, st.option_type_id, st.sku, st.default_title, st.configure_quality, st.stone_group
)

SELECT *
FROM data_stone

UNION ALL

SELECT
    -1 AS stone_id,
    '-1' AS option_id,
    '-1' AS option_type_id,
    'Not Defined' AS sku,
    'Not Defined' AS default_title,
    'Not Defined' AS configure_quality,
    'Not Defined' AS stone_group
