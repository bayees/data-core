{{ 
	config(materialized='external', 
	location='s3://' ~ env_var('ENVIRONMENT', '') ~ '/reporting/dagster/location.parquet') 
}}

select
	*
from {{ ref("dim_location") }}