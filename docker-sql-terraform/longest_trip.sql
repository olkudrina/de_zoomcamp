select lpep_pickup_datetime::date 
from taxi_data
where trip_distance = (select max(trip_distance) from taxi_data)