{{ 
	config(materialized='external', 
	location='s3://' ~ env_var('ENVIRONMENT', '') ~ '/curated/shared/dim_time.parquet') 
}}

with
time_sequence as (
    select 
       '1900-01-01 00:00:00'::datetime + interval (generated_number) second as generated_number
    from {{ ref('enriched_number_series')}}
)

select 
    strftime(generated_number, '%H:%M:%S') as time_id,
    datepart('hour', generated_number) as hour_int,
    datepart('minute', generated_number) as minute_int,
    datepart('second', generated_number) as second_int,
    concat( right ( concat('00', datepart('hour', generated_number)), 2 ), ':00:00' ) as hour_string,
    concat( right ( concat('00', datepart('hour', generated_number)), 2 ), ':', right ( concat('00', datepart('minute', generated_number)), 2 ), ':00' ) as minute_string,
    concat( right ( concat('00', datepart('hour', generated_number)), 2 ), ':', right ( concat('00', datepart('minute', generated_number)), 2 ), ':',  right ( concat('00', datepart('second', generated_number)), 2 )) as second_string,
    case
        when 23 <= datepart('hour', generated_number) or datepart('hour', generated_number) < 5 then 'night'
        when 5 <= datepart('hour', generated_number) and datepart('hour', generated_number) < 12 then 'morning'
        when 9 <= datepart('hour', generated_number) and datepart('hour', generated_number) < 18 then 'noon'
        when 18 <= datepart('hour', generated_number) and datepart('hour', generated_number) < 23 then 'evening'
    end as time_of_day
from time_sequence
