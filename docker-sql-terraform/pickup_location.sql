select zone, sum(total_amount) as total_amount
from taxi_data
inner join taxi_zones on pulocationid = locationid
where lpep_pickup_datetime::date = '2019-10-18'::date
group by zone
having sum(total_amount) >= 13000
order by 2 desc