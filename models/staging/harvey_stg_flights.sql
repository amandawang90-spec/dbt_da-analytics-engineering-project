{{ config(materialized = 'table') }}

SELECT 
    *
FROM {{ source('flights_data', 'flights') }} AS f
WHERE f.flight_date::DATE BETWEEN '2017-07-28' AND '2017-09-28'
ORDER BY f.flight_date