{{ 
	config(materialized='external', 
	location='s3://' ~ env_var('ENVIRONMENT', '') ~ '/reporting/evidence/lego_collection.parquet') 
}}

select
    set.* exclude (lego_set_id),
    part.* exclude (lego_part_id),
    parts.* exclude (lego_set_id, lego_part_id)
from {{ ref("fct_lego_parts") }} as parts
left join {{ ref("dim_lego_set") }} as set using (lego_set_id)
left join {{ ref("dim_lego_part") }} as part using (lego_part_id)