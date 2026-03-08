SELECT
    product_category_name,
    product_category_name_english
FROM {{ source('olist_raw_sources', 'product_category_name_translation') }}