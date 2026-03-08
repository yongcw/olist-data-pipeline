SELECT
    product_id,
    product_category_name
FROM {{ source('olist_raw_sources', 'olist_products_dataset') }}