{{ 
	config(materialized='external', 
	location='s3://' ~ env_var('ENVIRONMENT', '') ~ '/curated/lego/dim_lego_part.parquet') 
}}

select distinct
    {{ dbt_utils.generate_surrogate_key(['inv_part_id']) }} as lego_part_id,
	inv_part_id as lego_inventory_number,
	part__part_num as lego_part_number,
	part__name as lego_part_name,
	part__part_url as lego_part_url,
	part__part_img_url as lego_part_image_url,
	color__name as lego_part_color_name,
	color__rgb as lego_part_color_rgb,
	color__is_trans as lego_part_color_is_transparent
from {{ source('rebrickable', 'lego_parts') }}