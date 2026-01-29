{{ config(materialized = 'table') }}

WITH flights_adjustment AS (
       SELECT 
           hsf.*,
           (dep_delay * '1 minute'::interval) AS dep_delay_interval,
           (arr_delay * '1 minute'::interval) AS arr_delay_interval,
           (distance / 0.621371) AS distance_km,
           flight_date + ( (sched_dep_time / 100)::int * interval '1 hour' ) + ( (sched_dep_time % 100)::int * interval '1 minute' ) AS flight_datetime,
           flight_date + ( (sched_dep_time / 100)::int * interval '1 hour' ) AS flight_datetime_hour
       FROM {{ ref('harvey_stg_flights') }} as hsf
)
SELECT *
FROM flights_adjustment AS fa
