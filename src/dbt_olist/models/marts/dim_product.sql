SELECT
    p.product_id,
    p.product_category_name AS category_name_pt,
    COALESCE(t.product_category_name_english, 'unknown') AS category_name_english
FROM {{ ref('stg_products') }} p
LEFT JOIN {{ ref('stg_product_translation') }} t
    ON p.product_category_name = t.product_category_name