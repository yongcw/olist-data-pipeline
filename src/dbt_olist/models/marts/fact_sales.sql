WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),
items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),
customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

sales_joined AS (
    SELECT 
        i.order_item_id,
        o.order_id,
        i.product_id,
        i.seller_id,
        c.customer_unique_id,
        o.order_purchase_at,
        FORMAT_DATE('%Y%m%d', DATE(o.order_purchase_at)) AS date_id,
        i.price,
        i.freight_value,
        -- Requirement: Derived column for total sale amount
        (i.price + i.freight_value) AS total_sale_amount
    FROM items i
    LEFT JOIN orders o ON i.order_id = o.order_id
    LEFT JOIN customers c ON o.customer_id = c.customer_id
    WHERE o.order_status = 'delivered'
)

SELECT 
    *,
    -- Requirement: Derived column for CLV using a window function
    SUM(total_sale_amount) OVER (
        PARTITION BY customer_unique_id 
    ) AS customer_lifetime_value
FROM sales_joined