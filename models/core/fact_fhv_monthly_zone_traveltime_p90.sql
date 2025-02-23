{{
    config(
        materialized='table'
    )
}}

WITH temp_fhv AS (
    SELECT
        pickup_datetime,
        drop_off_datetime,
        pickup_location_id,
        dropoff_location_id,
        pickup_zone,
        dropoff_zone,
        year,
        month,
        TIMESTAMP_DIFF(drop_off_datetime, cast(pickup_datetime as timestamp), SECOND) AS trip_duration
    FROM {{ ref('dim_fhv_zones') }}
)
SELECT distinct
    pickup_location_id, dropoff_location_id,
    pickup_zone, dropoff_zone,
    year, month, trip_duration,
    PERCENTILE_CONT(trip_duration, 0.9) OVER (PARTITION BY year, month, pickup_location_id, dropoff_location_id) AS trip_duration_p90
FROM temp_fhv