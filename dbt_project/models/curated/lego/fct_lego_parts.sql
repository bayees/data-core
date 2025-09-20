{{ 
	config(materialized='external', 
	location='s3://' ~ env_var('ENVIRONMENT', '') ~ '/curated/lego/fct_lego_parts.parquet') 
}}

select
    {{ dbt_utils.generate_surrogate_key(['lego_parts.set_num']) }} as lego_set_id,
    {{ dbt_utils.generate_surrogate_key(['lego_parts.inv_part_id']) }} as lego_part_id,
    lego_parts.quantity as lego_part_quantity,
    lego_sets.num_parts as lego_set_total_quantity,
from {{ source('rebrickable', 'lego_parts') }} as lego_parts
left join {{ source('rebrickable', 'lego_sets') }} as lego_sets 
    on lego_parts.set_num = lego_sets.set_num    