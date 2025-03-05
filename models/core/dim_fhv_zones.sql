{{
    config(
        materialized='table'
    )
}}

with dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    dispatching_base_num,
    sr_flag,
    affiliated_base_number
    pickup_datetime,
    pickup_location_id,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    drop_off_datetime,
    dropoff_location_id,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,
    extract(year from pickup_datetime) as year,
    extract(month from pickup_datetime) as month
from  {{ ref('stg_fhv') }}
inner join dim_zones as pickup_zone
on pickup_location_id = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on dropoff_location_id = dropoff_zone.locationid