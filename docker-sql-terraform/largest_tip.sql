with pickup_filter_data as (
    select * from taxi_data
    where pulocationid = (select locationid from taxi_zones where zone = 'East Harlem North')
    and date_trunc('month', lpep_pickup_datetime::date) = '2019-10-01'::date
)
select zone
from pickup_filter_data
inner join taxi_zones on dolocationid = locationid
where tip_amount = (select max(tip_amount) from pickup_filter_data) 