{{ 
  config(
    materialized = 'table',
    description = 'Hourly Distribution of Trip Duration Percentiles by User Type (in minutes)'
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
  ROUND(
    approx_quantiles(trip_duration_seconds, 100)[OFFSET(10)] / 60, 2
  ) AS p10_minutes,
  ROUND(
    approx_quantiles(trip_duration_seconds, 100)[OFFSET(50)] / 60, 2
  ) AS median_minutes,
  ROUND(
    approx_quantiles(trip_duration_seconds, 100)[OFFSET(90)] / 60, 2
  ) AS p90_minutes
FROM hourly_durations
GROUP BY
  start_hour,
  member_casual
ORDER BY
  start_hour,
  member_casual

-- APPROX_QUANTILES(trip_duration_seconds, N) computes an array of 
-- N+1 quantile boundaries over the input values.