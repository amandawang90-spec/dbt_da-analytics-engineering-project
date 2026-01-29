{{ config(materialized = 'table') }}

SELECT 
    hmdfah.airport_code,
    hmdfah.flight_datetime_hour,
    hmdfah.name,
    hmdfah.city,
    hmdfah.country,
    hmdfah.total_planned_flights,
    hmdfah.total_planned_departure_flights,
    hmdfah.total_planned_arrival_flights,
    hmdfah.total_cancelled_flights,
    hmdfah.total_cancelled_departure_flights,
    hmdfah.total_cancelled_arrival_flights,
    hmdfah.total_diverted_flights,
    hmdfah.total_diverted_departure_flights,
    hmdfah.total_diverted_arrival_flights,
    hmdfah.total_actual_flights,
    hmdfah.total_actual_departure_flights,
    hmdfah.total_actual_arrival_flights,
    hmdfah.total_cancellation_percentage,
    hmdfah.departure_cancellation_percentage,
    hmdfah.arrival_cancellation_percentage,
    hmdfah.total_diversion_percentage,
    hmdfah.departure_diversion_percentage,
    hmdfah.arrival_diversion_percentage,
    hmdfah.total_unique_departure_airplanes,
    hmdfah.total_unique_arrival_airplanes,
    hmdfah.total_unique_departure_airlines,
    hmdfah.total_unique_arrival_airlines,
    hmdfah.avg_dep_delay,
    hmdfah.avg_arr_delay,
    hpwh.avg_temp_c,
    hpwh.precipitation_mm,
    hpwh.avg_wind_speed,
    hpwh.avg_wind_direction,
    hpwh.avg_pressure_hpa
FROM {{ ref('harvey_mart_flights_airport_hourly') }}  as hmdfah
JOIN {{ ref('harvey_prep_weather_hourly') }} as hpwh
ON hmdfah.flight_datetime_hour = hpwh.timestamp
ORDER BY airport_code, flight_datetime_hour