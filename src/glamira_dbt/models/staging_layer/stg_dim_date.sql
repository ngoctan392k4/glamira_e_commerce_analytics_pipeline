WITH dim_date_source AS (
    SELECT *
    FROM {{source('glamira_src', 'raw_glamira_behaviour')}}
),

dim_date_info AS (
    SELECT DISTINCT
        FORMAT_DATE('%Y%m%d', DATE(TIMESTAMP_SECONDS(ddc.time_stamp))) AS date_id,
        DATE(TIMESTAMP_SECONDS(ddc.time_stamp)) AS full_date,
        FORMAT_DATE('%A', DATE(TIMESTAMP_SECONDS(ddc.time_stamp))) AS date_of_week,
        FORMAT_DATE('%a', DATE(TIMESTAMP_SECONDS(ddc.time_stamp))) AS date_of_week_short,

        CASE
            WHEN EXTRACT(DAYOFWEEK FROM TIMESTAMP_SECONDS(ddc.time_stamp)) in (1, 7) THEN 'weekend'
            ELSE 'weekday'
        END AS is_weekday_or_weekend,

        EXTRACT(DAY FROM TIMESTAMP_SECONDS(ddc.time_stamp)) AS day_of_month,
        EXTRACT(DAYOFYEAR FROM TIMESTAMP_SECONDS(ddc.time_stamp)) AS day_of_year,
        EXTRACT(WEEK FROM TIMESTAMP_SECONDS(ddc.time_stamp)) AS week_of_year,
        EXTRACT(QUARTER FROM TIMESTAMP_SECONDS(ddc.time_stamp)) AS quarter_number,
        EXTRACT(YEAR FROM TIMESTAMP_SECONDS(ddc.time_stamp)) AS year_number,
        FORMAT_DATE('%Y%m', DATE(TIMESTAMP_SECONDS(ddc.time_stamp))) AS year_month

    FROM dim_date_source ddc
)

SELECT *
FROM dim_date_info
