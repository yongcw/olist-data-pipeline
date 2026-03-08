SELECT
    FORMAT_DATE('%Y%m%d', d) AS date_id,
    d AS full_date,
    EXTRACT(YEAR FROM d) AS year,
    EXTRACT(MONTH FROM d) AS month,
    EXTRACT(DAY FROM d) AS day,
    FORMAT_DATE('%Y-%m', d) AS year_month
FROM UNNEST(GENERATE_DATE_ARRAY('2016-01-01', '2019-12-31', INTERVAL 1 DAY)) AS d