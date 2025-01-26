select 
    count(*) filter(where trip_distance <= 1) as up_to_1_mile,
    count(*) filter(where trip_distance > 1 and trip_distance <= 3) as trip_1_3_mile,
    count(*) filter(where trip_distance > 3 and trip_distance <= 7) as trip_3_7_mile,
    count(*) filter(where trip_distance > 7 and trip_distance <= 10) as trip_7_10_mile,
    count(*) filter(where trip_distance > 10) as over_10_mile
from taxi_data