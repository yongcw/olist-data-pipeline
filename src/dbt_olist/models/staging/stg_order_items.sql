SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_at,
    CAST(price AS FLOAT64) AS price,
    CAST(freight_value AS FLOAT64) AS freight_value
FROM {{ source('olist_raw_sources', 'olist_order_items_dataset') }}