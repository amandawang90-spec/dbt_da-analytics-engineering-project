{{ config(materialized = 'table') }}

WITH daily_aggregation AS (
    SELECT 
        airport_code,
        DATE(timestamp) AS date,
        AVG(avg_temp_c)::NUMERIC(4,2) AS avg_temp_c,
        AVG(humidity_in_percent)::NUMERIC(4,2) AS avg_humidity_in_percent,
        SUM(precipitation_mm) AS total_prec_mm,
        AVG(avg_wind_direction)::NUMERIC(5,2) AS avg_wind_direction,
        AVG(avg_wind_speed)::NUMERIC(5,2) AS avg_wind_speed_kmh,
        AVG(avg_pressure_hpa)::NUMERIC(6,2) AS avg_pressure_hpa
    FROM {{ ref('harvey_prep_weather_hourly') }}
    GROUP BY airport_code, DATE(timestamp)
)
SELECT *
FROM daily_aggregation
ORDER BY airport_code, date