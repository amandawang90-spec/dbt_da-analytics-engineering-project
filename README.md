# neuefische_da_180825_analytics_engineering_project

## Overview
This project analyzes flights and weather data during the Harvey hurricane period. It provides insights into flight performance, delays, cancellations, diversions, and the impact of weather. The project is implemented using dbt, with **Staging, Prep, and Mart layers** to create clean, tested, and analysis-ready datasets.

## Data Sources
The project uses two main sources:

1. Weather Data
   - `harvey_weather_hourly_raw_data`: Raw hourly weather observations from airport stations.
   - `harvey_weather_daily_raw_data`: Raw daily weather observations.
   - Metrics include temperature, precipitation, wind speed/direction, pressure, and sunlight.

2. Flights Data
   - `harvey_flights_all`: Raw flight records, including scheduled and actual times, delays, cancellations, diversions, airline, and aircraft info.
   - `airports`: Airport metadata including location, region, and timezone.
   - `regions`: Regional metadata for grouping airports.

## Project Structure

### Staging Models (`harvey_stg_*`)
Staging models are the first layer after raw sources. Their purpose is to:

- Flatten nested JSON or raw structures
- Standardize column names and types
- Prepare clean, canonical tables for further enrichment

Staging Models Include:

- `harvey_stg_weather_hourly`: Flattened hourly weather data
- `harvey_stg_weather_daily`: Flattened daily weather data
- `harvey_stg_airports`: Standardized airport table
- `harvey_stg_flights`: Canonical flight table combining flights and airport info

### Prep Models (`harvey_prep_*`)
Prep models build on staging models. They enrich the data by:

- Adding derived metrics (e.g., `distance_km`, `dep_delay_interval`, `flight_datetime`, `flight_datetime_hour`)
- Adding temporal attributes (`day`, `week`, `month`, `year`, `season`, `day_part`)
- Separating departures and arrivals when needed
- Making data ready for marts

Prep Models Include:

Flights Prep Models
- `harvey_prep_flights`: All flights (departures + arrivals)
- `harvey_prep_depature_flights`: Only departure flights
- `harvey_prep_arrival_flights`: Only arrival flights

Airports Prep Model
- `harvey_prep_airports`: Airport metadata cleaned and standardized

Weather Prep Models
- `harvey_prep_weather_hourly`: Hourly weather metrics with temporal attributes
- `harvey_prep_weather_daily`: Daily weather metrics aggregated from hourly

Mart Models (`harvey_mart_*`)
Mart models are analysis-ready datasets, aggregated for reporting, dashboards, and advanced analytics.

Flight Performance Marts include:
- Hourly:
  - `harvey_mart_flights_airport_hourly`
  - `harvey_mart_depature_flights_airport_hourly`
  - `harvey_mart_arrival_flights_airport_hourly`
- Daily:
  - `harvey_mart_flights_airport_daily`
  - `harvey_mart_depature_flights_airport_daily`
  - `harvey_mart_arrival_flights_airport_daily`

Flight x Weather Integration**
- Hourly: `harvey_mart_depature_flights_airport_hourly_weather`
- Daily: `harvey_mart_arrival_flights_airport_daily_weather`
- Daily aggregation: `harvey_mart_flights_airport_hourly_to_daily_aggregation_weather`

Weather Aggregations
- `harvey_mart_weather_hourly_to_daily_aggregation`: Aggregates hourly weather to daily metrics

Route Statistics
- `harvey_mart_route_stats`: Aggregated metrics by origin-destination routes, including flight counts, cancellations, diversions, unique airlines/airplanes, and delays

### Key Metrics Captured
- Flight Metrics
  - Planned, actual, cancelled, diverted flights
  - Average departure and arrival delays
  - Delay intervals (0-15, 16-30 minutes, etc.)
- Aircraft & Airlines
  - Unique airplanes and airlines per airport or route
- Weather Metrics
  - Temperature, precipitation, wind speed/direction, pressure, sunlight
- Temporal Dimensions
  - Hour, day, week, month, season, day part

## DBT Features
- Documentation: All models and columns documented in `schema.yml`
- Testing: Not-null, unique, and logical tests applied to key fields
- Aggregation Layers: Hourly → Daily, Flight × Weather
- Derived Metrics: Distance in km, departure/arrival delay intervals, cancellation/diversion percentages

## Usage
1. Build all models:
   ```bash
   dbt run

