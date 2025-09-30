WITH stone_in_product AS (
    SELECT distinct
        lower(st.sku) as sku,
        -- st.sku,
        ARRAY_AGG(default_title ORDER BY
            CASE WHEN REGEXP_CONTAINS(default_title, r'^(Black Diamond|Zirconia|Brilliant|Fire Opal|Green Diamond|Sapphire|Smoky Quartz|Sultan Stone)$')
                THEN 0 ELSE 1 END,
            LENGTH(default_title)
        )[OFFSET(0)] AS default_title,
        ANY_VALUE(st.configure_quality) AS configure_quality,
        ANY_VALUE(st.stone_group) AS stone_group
    FROM {{ref("stg_dim_product")}} dsc
    CROSS JOIN UNNEST(dsc.stone) AS st
    -- WHERE dsc.min_price LIKE '$%'
    GROUP BY st.sku
    order by sku
),

stone_in_raw AS (
    SELECT distinct
        lower(cp.value_label) as stone_sku
    FROM {{source("glamira_src", "raw_glamira_behaviour")}} fsc
    CROSS JOIN UNNEST(fsc.option) as cp
    WHERE cp.option_label is not NULL
        AND cp.option_label not in ('alloy','pear')
        AND cp.value_label is not NULL
        AND length(cp.value_label) >0

    UNION  DISTINCT

    SELECT distinct lower(cp.diamond)  as stone_sku
    FROM {{source("glamira_src", "raw_glamira_behaviour")}} fsc
    CROSS JOIN UNNEST(fsc.option) as cp
    WHERE cp.diamond is not NULL
),

data_stone AS (
    SELECT DISTINCT
        -- FARM_FINGERPRINT(st.option_id || '-' || st.option_type_id) AS stone_id,
        FARM_FINGERPRINT(st.default_title) AS stone_id,  

        -- Not use option id and option type id since same sku but different these ids => surplus
        -- st.option_id AS stone_type_id,
        -- st.option_type_id AS stone_name_id,
        st.sku,
        st.default_title AS stone_name,
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

    FROM stone_in_product st
    -- INNER JOIN stone_in_raw sir
    --     ON st.sku = sir.stone_sku
    -- CROSS JOIN UNNEST(st.data_stones) AS s
    -- GROUP BY FARM_FINGERPRINT(st.option_id || '-' || st.option_type_id), st.option_id, st.option_type_id, st.sku, st.default_title, st.configure_quality, st.stone_group

    UNION ALL

    SELECT DISTINCT
        FARM_FINGERPRINT(sir.stone_sku) AS stone_id,
        sir.stone_sku AS sku,
        sir.stone_sku AS stone_name,
        'Not Defined' AS configure_quality,
        'Not Defined' AS stone_group
    FROM stone_in_raw sir
    LEFT JOIN stone_in_product st
        ON sir.stone_sku = st.sku
    WHERE st.sku IS NULL


),

stone AS (
    SELECT *
    FROM data_stone
    WHERE sku <> ''

    UNION ALL

    SELECT
        -1 AS stone_id,
        'Not Defined' AS sku,
        'Not Defined' AS stone_name,
        'Not Defined' AS configure_quality,
        'Not Defined' AS stone_group
)

select *
from stone
QUALIFY ROW_NUMBER() OVER (PARTITION BY stone_name ORDER BY stone_name) = 1
