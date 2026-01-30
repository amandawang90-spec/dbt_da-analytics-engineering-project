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
    hmwhtd.avg_temp_c,
    hmwhtd.total_prec_mm,
    hmwhtd.avg_wind_speed_kmh,
    hmwhtd.avg_wind_direction,
    hmwhtd.avg_pressure_hpa
FROM {{ ref('harvey_mart_flights_airport_daily') }} AS hmdfad
LEFT JOIN {{ ref('harvey_mart_weather_hourly_to_daily_aggregation') }} AS hmwhtd
     ON hmdfad.airport_code = hmwhtd.airport_code
    AND hmdfad.flight_date = hmwhtd.date
ORDER BY airport_code, flight_date




