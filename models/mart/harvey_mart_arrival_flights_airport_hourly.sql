{{ config(materialized = 'table') }}

WITH arrivals AS ( 
    SELECT 
        dest AS airport_code,
        flight_datetime_hour,
        COUNT(sched_arr_time) AS total_planned_arrival_flights,
        SUM(cancelled) AS total_cancelled_arrival_flights,
        SUM(diverted) AS total_diverted_arrival_flights,
        COUNT(arr_time) AS total_actual_arrival_flights,
		COUNT(DISTINCT tail_number) AS total_unique_arrival_airplanes,
		COUNT(DISTINCT airline) AS total_unique_arrival_airlines,
        AVG(arr_delay) as avg_arr_delay
    FROM {{ ref('harvey_prep_flights') }} as hpf
    WHERE hpf.dest IN ('IAH', 'HOU', 'CRP', 'LCH', 'BPT')
    GROUP BY dest, flight_datetime_hour
),
add_names AS (
    SELECT 
        a.airport_code,
        a.flight_datetime_hour,
        hpa.name,
        hpa.city,
        hpa.country,
        a.total_planned_arrival_flights,
        a.total_cancelled_arrival_flights,
        a.total_diverted_arrival_flights,
        a.total_actual_arrival_flights,
        a.total_unique_arrival_airplanes,
        a.total_unique_arrival_airlines,
        a.avg_arr_delay 
    FROM arrivals as a
    LEFT JOIN {{ ref('harvey_prep_airports') }} hpa
      ON a.airport_code = hpa.faa
)
SELECT 
    an.airport_code,
    an.flight_datetime_hour,
    an.name,
    an.city,
    an.country,
    an.total_planned_arrival_flights,
    an.total_cancelled_arrival_flights,
    an.total_diverted_arrival_flights,
    an.total_actual_arrival_flights,
    an.total_unique_arrival_airplanes,
    an.total_unique_arrival_airlines,
    an.avg_arr_delay
FROM add_names an
ORDER BY an.airport_code, an.flight_datetime_hour