{{ 
	config(materialized='external', 
	location='s3://' ~ env_var('ENVIRONMENT', '') ~ '/curated/lego/dim_lego_minifig.parquet') 
}}

select distinct
    {{ dbt_utils.generate_surrogate_key(['set_num']) }} as lego_minifig_id,
	set_num as lego_minifig_number,
	set_name as lego_minifig_name,
	set_img_url as lego_minifig_image_url
from {{ source('rebrickable', 'lego_minifigs') }}