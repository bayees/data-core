{{ 
	config(materialized='external', 
	location='s3://' ~ env_var('ENVIRONMENT', '') ~ '/curated/economy/dim_category.parquet') 
}}

with categories as (

    select * from {{ source('spiir', 'transactions') }}

)
, prepared_categories as (

    select distinct     
        {{ dbt_utils.generate_surrogate_key(['category_name']) }} as category_id,
        category_name,
        main_category_name,
        category_type,
        expense_type,
    from categories
    where category_name <> 'None'

)

select *
from prepared_categories