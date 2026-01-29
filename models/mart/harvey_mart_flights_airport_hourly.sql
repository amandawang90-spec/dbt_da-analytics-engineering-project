{{ config(materialized = 'table') }}

WITH departures_arrivals AS (
    SELECT
        COALESCE(d.airport_code, a.airport_code) AS airport_code,
        COALESCE(d.flight_datetime_hour, a.flight_datetime_hour) AS flight_datetime_hour,
        COALESCE(d.name, a.name) AS name,
        COALESCE(d.city, a.city) AS city,
        COALESCE(d.country, a.country) AS country,
        -- departure metrics
        d.total_planned_departure_flights,
        d.total_cancelled_departure_flights,
        d.total_diverted_departure_flights,
        d.total_actual_departure_flights,
        d.total_unique_departure_airplanes,
        d.total_unique_departure_airlines,
        d.avg_dep_delay,
        -- arrival metrics
        a.total_planned_arrival_flights,
        a.total_cancelled_arrival_flights,
        a.total_diverted_arrival_flights,
        a.total_actual_arrival_flights,
        a.total_unique_arrival_airplanes,
        a.total_unique_arrival_airlines,
        a.avg_arr_delay,
        -- combined totals
        COALESCE(d.total_planned_departure_flights,0) + COALESCE(a.total_planned_arrival_flights,0) AS total_planned_flights,
        COALESCE(d.total_cancelled_departure_flights,0) + COALESCE(a.total_cancelled_arrival_flights,0) AS total_cancelled_flights,
        COALESCE(d.total_diverted_departure_flights,0) + COALESCE(a.total_diverted_arrival_flights,0) AS total_diverted_flights,
        COALESCE(d.total_actual_departure_flights,0) + COALESCE(a.total_actual_arrival_flights,0) AS total_actual_flights,
        -- cancellation percentages
        (COALESCE(d.total_cancelled_departure_flights,0) + COALESCE(a.total_cancelled_arrival_flights,0))::numeric
            / NULLIF(COALESCE(d.total_planned_departure_flights,0) + COALESCE(a.total_planned_arrival_flights,0), 0) * 100 AS total_cancellation_percentage,
        COALESCE(d.total_cancelled_departure_flights,0)::numeric
            / NULLIF(d.total_planned_departure_flights,0) * 100 AS departure_cancellation_percentage,
        COALESCE(a.total_cancelled_arrival_flights,0)::numeric
            / NULLIF(a.total_planned_arrival_flights,0) * 100 AS arrival_cancellation_percentage,
        -- diversion percentages
        (COALESCE(d.total_diverted_departure_flights,0) + COALESCE(a.total_diverted_arrival_flights,0))::numeric
            / NULLIF(COALESCE(d.total_planned_departure_flights,0) + COALESCE(a.total_planned_arrival_flights,0),0) * 100 AS total_diversion_percentage,
        COALESCE(d.total_diverted_departure_flights,0)::numeric
            / NULLIF(d.total_planned_departure_flights,0) * 100 AS departure_diversion_percentage,
        COALESCE(a.total_diverted_arrival_flights,0)::numeric
            / NULLIF(a.total_planned_arrival_flights,0) * 100 AS arrival_diversion_percentage
    FROM {{ ref('harvey_mart_depature_flights_airport_hourly') }} d
    FULL OUTER JOIN {{ ref('harvey_mart_arrival_flights_airport_hourly') }} a
        USING (airport_code, flight_datetime_hour)
)
SELECT        
    airport_code,
    flight_datetime_hour,
    name,
    city,
    country,
    total_planned_flights,
    total_planned_departure_flights,
    total_planned_arrival_flights,
    total_cancelled_flights,
    total_cancelled_departure_flights,
    total_cancelled_arrival_flights,
    total_diverted_flights,
    total_diverted_departure_flights,
    total_diverted_arrival_flights,
    total_actual_flights,
    total_actual_departure_flights,
    total_actual_arrival_flights,
    total_cancellation_percentage,
    departure_cancellation_percentage,
    arrival_cancellation_percentage,
    total_diversion_percentage,
    departure_diversion_percentage,
    arrival_diversion_percentage,
    total_unique_departure_airplanes,
    total_unique_arrival_airplanes,
    total_unique_departure_airlines,
    total_unique_arrival_airlines,
    avg_dep_delay,
    avg_arr_delay
FROM departures_arrivals
ORDER BY airport_code, flight_datetime_hour;