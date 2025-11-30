{{
    config(
        materialized='view'
    )
}}

with
source as (
   select * from {{ source('staging', 'divvy_tripdata_merged') }}
),
renamed as (
    select
        CAST(filename AS STRING) AS filename,
        CAST(ride_id AS BYTES) AS ride_id,
        CAST(rideable_type AS STRING) AS rideable_type,
        CAST(started_at AS TIMESTAMP) AS start_datetime,
        CAST(ended_at AS TIMESTAMP) AS end_datetime,
        CAST(start_station_name AS STRING) AS start_station_name,
        CAST(start_station_id AS STRING) AS start_station_id,
        CAST(end_station_name AS STRING) AS end_station_name,
        CAST(end_station_id AS STRING) AS end_station_id,
        CAST(start_lat AS FLOAT64) AS start_lat,
        CAST(start_lng AS FLOAT64) AS start_lng,
        CAST(end_lat AS FLOAT64) AS end_lat,
        CAST(end_lng AS FLOAT64) AS end_lng,
        CAST(member_casual AS STRING) AS member_casual
    from source
)
select * from renamed

{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}