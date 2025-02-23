{{
    config(
        materialized='table'
    )
}}

with temp AS (
    select
        service_type, year, month,
        {{ calculate_percentile('fare_amount', 0.97) }} as p97,
        {{ calculate_percentile('fare_amount', 0.95) }} as p95,
        {{ calculate_percentile('fare_amount', 0.9) }} as p90
    FROM {{ ref('fact_trips') }}
    where 
        fare_amount > 0 AND trip_distance > 0
        AND payment_type_description IN ('Cash', 'Credit Card')
)

select distinct * from temp