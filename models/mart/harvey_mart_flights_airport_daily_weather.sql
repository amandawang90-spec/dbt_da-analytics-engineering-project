{{ config(materialized = 'table') }}

SELECT 
    hmdfad.airport_code,
    hmdfad.flight_date,
    hmdfad.name,
    hmdfad.city,
    hmdfad.country,
    hmdfad.total_planned_flights,
    hmdfad.total_planned_departure_flights,
    hmdfad.total_planned_arrival_flights,
    hmdfad.total_cancelled_flights,
    hmdfad.total_cancelled_departure_flights,
    hmdfad.total_cancelled_arrival_flights,
    hmdfad.total_diverted_flights,
    hmdfad.total_diverted_departure_flights,
    hmdfad.total_diverted_arrival_flights,
    hmdfad.total_actual_flights,
    hmdfad.total_actual_departure_flights,
    hmdfad.total_actual_arrival_flights,
    hmdfad.total_cancellation_percentage,
    hmdfad.departure_cancellation_percentage,
    hmdfad.arrival_cancellation_percentage,
    hmdfad.total_diversion_percentage,
    hmdfad.departure_diversion_percentage,
    hmdfad.arrival_diversion_percentage,
    hmdfad.total_unique_departure_airplanes,
    hmdfad.total_unique_arrival_airplanes,
    hmdfad.total_unique_departure_airlines,
    hmdfad.total_unique_arrival_airlines,
    hmdfad.avg_dep_delay,
    hmdfad.avg_arr_delay,
    hpwd.avg_temp_c,
    hpwd.precipitation_mm,
    hpwd.avg_wind_speed_kmh,
    hpwd.avg_wind_direction,
    hpwd.avg_pressure_hpa
FROM {{ ref('harvey_mart_flights_airport_daily') }} AS hmdfad
LEFT JOIN {{ ref('harvey_prep_weather_daily') }} AS hpwd
     ON hmdfad.airport_code = hpwd.airport_code
    AND hmdfad.flight_date = hpwd.date
ORDER BY airport_code, flight_date

