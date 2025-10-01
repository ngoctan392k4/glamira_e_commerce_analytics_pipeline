{{ config(
    schema='glamira_analysis',
    alias='fact_sales',
    materialized='incremental'
) }}

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
    END AS total_in_usd,

    {% if is_incremental() %}
        f.date_inserted,
        current_timestamp as date_updated
    {% else %}
        current_timestamp as date_inserted,
        current_timestamp as date_updated
    {% endif %}

FROM {{ref("stg_fact_sales")}} fs
LEFT JOIN {{ref("stg_dim_location")}} sls
    ON fs.ip_address = sls.ip_address

{% if is_incremental() %}
LEFT JOIN {{ this }} f
    on fs.sale_id = f.sale_id
{% endif %}