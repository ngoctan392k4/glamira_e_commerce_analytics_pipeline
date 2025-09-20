{{ config(
    materialized='table'
) }}

WITH dim_color_source AS (
    SELECT *
    FROM {{source('glamira_src', 'raw_glamira_behaviour')}}
),

dim_color_code_value AS (
    SELECT DISTINCT
        opt.value_label as alloy_value,
        CASE
            WHEN STRPOS(opt.value_label, '-') > 0 THEN SUBSTR(opt.value_label, 1, STRPOS(opt.value_label, '-')-1)
            ELSE opt.value_label
        END AS color_code
    FROM dim_color_source
    JOIN UNNEST(option) AS opt
    WHERE opt.option_label = 'alloy'
),

dim_color AS (
    SELECT *
    FROM dim_color_code_value
    WHERE
        color_code IS NOT NULL
        AND color_code <> ''
)

SELECT distinct
    FARM_FINGERPRINT(color_code) as color_id,
    color_code,
    CASE
        WHEN color_code= 'white_yellow' THEN 'White Yellow'
        WHEN color_code= 'red' THEN 'Rose'
        WHEN color_code= 'yellow' THEN 'Yellow'
        WHEN color_code= 'white' THEN 'White'
        WHEN color_code= 'red_white' THEN 'Rose White'
        WHEN color_code= 'yellow_white' THEN 'Yellow White'
        WHEN color_code= 'white_red' THEN 'White Rose'
        WHEN color_code= 'yellow_white_red' THEN 'Yellow White Rose'
        WHEN color_code= 'white_yellow_red' THEN 'White Yellow Rose'
        WHEN color_code= 'red_white_yellow' THEN 'Rose White Yellow'
        WHEN color_code= 'black_yellow' THEN 'Black/Yellow'
        WHEN color_code= 'black_white' THEN 'Black/White'
        WHEN color_code= 'black_red' THEN 'Black/Rose'
        WHEN color_code= 'natural_red' THEN 'Rose'
        WHEN color_code= 'natural_white' THEN 'White'
        WHEN color_code= 'natural_yellow' THEN 'Yellow'
        ELSE 'Undefined'
    END as color_name
FROM dim_color

UNION ALL

SELECT
    -1 AS color_id,
    'Not Defined' AS color_code,
    'Not Defined' AS color_name