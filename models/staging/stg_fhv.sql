{{
    config(
        materialized='view'
    )
}}

select 
    dispatching_base_num, pickup_datetime,
    dropOff_datetime as drop_off_datetime,
    PUlocationID as pickup_location_id,
    DOlocationID as dropoff_location_id,
    SR_Flag as sr_flag,
    Affiliated_base_number as affiliated_base_number
from {{ source('staging','fhv') }}
where dispatching_base_num is not null 


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}