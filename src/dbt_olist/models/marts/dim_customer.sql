SELECT
    customer_unique_id,
    MAX(zip_code) AS zip_code,
    MAX(city) AS city,
    MAX(state) AS state,
    COUNT(DISTINCT customer_id) AS total_orders_placed
FROM {{ ref('stg_customers') }}
GROUP BY 1