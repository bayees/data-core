{{ 
	config(materialized='external', 
	location='s3://' ~ env_var('ENVIRONMENT', '') ~ '/curated/lego/fct_lego_minifig_inventory.parquet') 
}}


select distinct
    {{ dbt_utils.generate_surrogate_key(['set_num']) }} as lego_minifig_id,
	{{ dbt_utils.generate_surrogate_key(['_inventory_sets_set_num']) }} as lego_set_id,
	quantity as lego_minifig_quantity 
from {{ source('rebrickable', 'lego_minifigs') }}