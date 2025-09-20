{{ 
	config(
        materialized='external', 
	    location='s3://' ~ env_var('ENVIRONMENT', '') ~ '/curated/location_tracking/fct_positions.parquet'
    )
}}

with positions as (

    select * from {{ source('home_assistant', 'states__value') }}

)
, prepared_positions as (

    select 
        {{ dbt_utils.generate_surrogate_key(['attributes__latitude', 'attributes__longitude']) }} as location_id,
        attributes__gps_accuracy as gps_accuracy_meters,
        last_changed as create_at,
        lag(last_changed, 1) over (order by last_changed) as last_create_at,
        attributes__latitude as latitude_degrees,
        attributes__longitude as longitude_degrees,
        lag(latitude_degrees, 1) over (order by create_at)  AS last_latitude_degrees,
        lag(longitude_degrees, 1) over (order by create_at)  AS last_longitude_degrees,
        attributes__speed as speed_meters_per_second,
        attributes__course as course_degrees,
        attributes__altitude as altitude_meters,
        attributes__vertical_accuracy as vertical_accuracy,
        state as location
    from positions

)
, calculated_durations as (
    
    select
        location_id,
        gps_accuracy_meters,
        create_at,
        create_at::date as event_date_id,
        strftime(create_at at time zone 'Europe/Copenhagen', '%H:%M:%S') as event_time_id,
        last_create_at as start_event_at,
        create_at as end_event_at,
        date_diff('minute', last_create_at, create_at) as duration_minutes,
        speed_meters_per_second * 3.6 as speed_kilometers_per_hour,
        course_degrees,
        case
            when location = 'not_home' then 'Unknown'
            when location = 'home' then 'Home'
            else location
        end as known_location,
        st_distance_spheroid(
            st_point(last_latitude_degrees, last_longitude_degrees), 
            st_point(latitude_degrees, longitude_degrees))
        AS aerial_distance, 
    from prepared_positions
    
)

select 
    *
from calculated_durations