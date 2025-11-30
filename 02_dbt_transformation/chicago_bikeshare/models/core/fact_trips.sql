{{
    config(
        materialized='table'
    )
}}

with divvy_tripdata as (
    select *
    from {{ ref('stg_divvy_tripdata') }}
)
select
  ride_id,
  rideable_type,
  start_datetime,
  end_datetime,
  start_station_name,
  start_station_id,
  end_station_name,
  end_station_id,
  start_lat,
  start_lng,
  end_lat,
  end_lng,
  member_casual,
  EXTRACT(YEAR FROM start_datetime) as start_year,
  EXTRACT(QUARTER FROM start_datetime) as start_quarter,
  EXTRACT(MONTH FROM start_datetime) as start_month,
  EXTRACT(DAYOFWEEK FROM start_datetime) as start_day_of_week,
  EXTRACT(HOUR FROM start_datetime) as start_hour,
  EXTRACT(WEEK FROM start_datetime) as start_week_of_year,
  TIMESTAMP_DIFF(end_datetime, start_datetime, SECOND) as trip_duration_seconds,
  CASE 
    WHEN EXTRACT(DAYOFWEEK FROM start_datetime) IN (1,7) THEN TRUE 
    ELSE FALSE 
  END as is_weekend,
  CASE
    WHEN TIMESTAMP_DIFF(end_datetime, start_datetime, SECOND) < 300 THEN 'short'
    WHEN TIMESTAMP_DIFF(end_datetime, start_datetime, SECOND) BETWEEN 300 AND 1800 THEN 'medium'
    ELSE 'long'
  END as trip_duration_category
FROM divvy_tripdata