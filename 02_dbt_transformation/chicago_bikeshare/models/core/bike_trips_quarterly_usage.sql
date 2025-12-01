{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fact_trips') }}
),
total_trips_quarter AS(
    SELECT 
      member_casual, 
      start_year, 
      CONCAT("Q", start_quarter) AS start_quarter, 
      COUNT(ride_id) AS count_trips 
    FROM trips_data
    WHERE start_datetime BETWEEN '2023-01-01 00:00:00 UTC' AND '2024-12-31 23:59:59 UTC'
    GROUP BY 
      member_casual, 
      start_year, 
      CONCAT("Q", start_quarter)
    ORDER BY member_casual, start_quarter, start_year ASC
)
SELECT 
  *, 
  ROUND(100*(count_trips - (LAG(count_trips) OVER (PARTITION BY member_casual, start_quarter ORDER BY start_year)))/ LAG(count_trips) OVER (PARTITION BY member_casual, start_quarter ORDER BY start_year),2) as YoY_growth 
FROM total_trips_quarter
ORDER BY member_casual, start_quarter, start_year