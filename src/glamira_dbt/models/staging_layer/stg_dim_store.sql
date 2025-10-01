WITH dim_store_source AS (
    SELECT DISTINCT
        dss.store_id,
        'Store ' || dss.store_id as store_name
    FROM {{source('glamira_src', 'raw_glamira_behaviour')}} dss

    UNION ALL

    SELECT
        '-1' AS store_id,
        'Not Defined' AS store_name
)

SELECT *
FROM dim_store_source