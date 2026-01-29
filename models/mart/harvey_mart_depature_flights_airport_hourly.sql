{{ config(materialized = 'table') }}

WITH departures AS ( 
    SELECT 
        origin AS airport_code,
        flight_datetime_hour,
        COUNT(sched_dep_time) AS total_planned_departure_flights,
        SUM(cancelled) AS total_cancelled_departure_flights,
        SUM(diverted) AS total_diverted_departure_flights,
        COUNT(dep_time) AS total_actual_departure_flights,
		COUNT(DISTINCT tail_number) AS total_unique_departure_airplanes,
	    COUNT(DISTINCT airline) AS total_unique_departure_airlines,
        AVG(dep_delay) as avg_dep_delay
    FROM {{ ref('harvey_prep_flights') }} as hpf
    WHERE hpf.origin IN ('IAH', 'HOU', 'CRP', 'LCH', 'BPT')
    GROUP BY hpf.origin, hpf.flight_datetime_hour
),
add_names AS (
    SELECT 
        d.airport_code,
        d.flight_datetime_hour,
        hpa.name,
        hpa.city,
        hpa.country,
        d.total_planned_departure_flights,
        d.total_cancelled_departure_flights,
        d.total_diverted_departure_flights,
        d.total_actual_departure_flights,
        d.total_unique_departure_airplanes,
        d.total_unique_departure_airlines,
        d.avg_dep_delay 
    FROM departures as d
    LEFT JOIN {{ ref('harvey_prep_airports') }} hpa 
      ON d.airport_code = hpa.faa
)
SELECT 
    an.airport_code,
    an.flight_datetime_hour,
    an.name,
    an.city,
    an.country,
    an.total_planned_departure_flights,
    an.total_cancelled_departure_flights,
    an.total_diverted_departure_flights,
    an.total_actual_departure_flights,
    an.total_unique_departure_airplanes,
    an.total_unique_departure_airlines,
    an.avg_dep_delay
FROM add_names an
ORDER BY an.airport_code, an.flight_datetime_hour