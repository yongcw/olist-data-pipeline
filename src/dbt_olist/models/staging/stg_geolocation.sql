SELECT
    geolocation_zip_code_prefix AS zip_code,
    CAST(geolocation_lat AS FLOAT64) AS latitude,
    CAST(geolocation_lng AS FLOAT64) AS longitude,
    geolocation_city AS city,
    geolocation_state AS state
FROM {{ source('olist_raw_sources', 'olist_geolocation_dataset') }}