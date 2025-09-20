{{ 
	config(materialized='external', 
	location='s3://' ~ env_var('ENVIRONMENT', '') ~ '/curated/lego/dim_lego_set.parquet') 
}}

select distinct
    {{ dbt_utils.generate_surrogate_key(['fields__set_number']) }} as lego_set_id,
	fields__set_number as lego_set_number,
	name as lego_set_name,
	year as lego_set_year,
    set_img_url as lego_set_image_url,
    set_url as lego_set_url
from {{ source('grist', 'sets') }} as grist_sets
left join {{ source('rebrickable', 'lego_sets') }} as rebrickable_sets 
    on grist_sets.fields__set_number = rebrickable_sets.set_num