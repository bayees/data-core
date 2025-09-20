{{ 
	config(materialized='external', 
	location='s3://' ~ env_var('ENVIRONMENT', '') ~ '/reporting/evidence/lego_minifigs.parquet') 
}}

select
    set.* exclude (lego_set_id),
    part.* exclude (lego_minifig_id),
    parts.* exclude (lego_set_id, lego_minifig_id)
from {{ ref("fct_lego_minifig_inventory") }} as parts
left join {{ ref("dim_lego_set") }} as set using (lego_set_id)
left join {{ ref("dim_lego_minifig") }} as part using (lego_minifig_id)