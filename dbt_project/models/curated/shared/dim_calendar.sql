{{ 
	config(materialized='external', 
	location='s3://' ~ env_var('ENVIRONMENT', '') ~ '/curated/shared/dim_calendar.parquet') 
}}

select 
    date_actual::date as calendar_id
    ,date_actual::date as date_actual
    ,day_actual
    ,day_zero_added
    ,weekday_actual
    ,weekday_name_short
    ,weekday_name_long
    ,month_actual
    ,month_zero_added
    ,month_name_short
    ,month_name_long
    ,quarter_actual
    ,quarter_name
    ,year_quarter_name
    ,day_of_year
    ,iso_week_of_year
    ,iso_year
    ,year
    ,first_day_of_month
    ,end_of_month
    ,first_day_of_year
    ,is_working_day
    ,holiday
from {{ source('calendar', 'calendar') }}
order by date_actual desc