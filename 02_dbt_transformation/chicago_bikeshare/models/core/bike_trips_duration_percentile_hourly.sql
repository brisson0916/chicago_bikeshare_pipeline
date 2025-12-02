{{ 
  config(
    materialized='table',
    description='Hourly Distribution of Trip Duration Percentiles by User Type'
  ) 
}}

WITH hourly_durations AS (
  SELECT
    start_hour,
    member_casual,
    trip_duration_seconds
  FROM {{ ref('fact_trips') }}
)

SELECT
  start_hour,
  member_casual,
  approx_quantiles(trip_duration_seconds, 100)[offset(10)] AS p10_seconds,
  approx_quantiles(trip_duration_seconds, 100)[offset(50)] AS median_seconds,
  approx_quantiles(trip_duration_seconds, 100)[offset(90)] AS p90_seconds
FROM hourly_durations
GROUP BY start_hour, member_casual
ORDER BY start_hour, member_casual

-- APPROX_QUANTILES(trip_duration_seconds, N) computes an array of 
-- N+1 quantile boundaries over the input values.