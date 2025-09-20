{{ 
	config(materialized='external', 
	location='s3://' ~ env_var('ENVIRONMENT', '') ~ '/curated/economy/fct_budget.parquet') 
}}

with 
unpivoted as (
    select 
        fields__year as year,
        replace(month, 'fields__', '') as month,
        fields__category as budget_category,
        fields__type as transaction_type,
        amount
    from (
        unpivot {{ source('grist', 'all') }}
        on "fields__jan", "fields__feb", "fields__mar", "fields__apr", "fields__may", "fields__jun", "fields__jul", "fields__aug", "fields__sep", "fields__oct", "fields__nov", "fields__dec" 
        into
            name month
            value amount
    )
), 
calendar_mapping as (
    select distinct
        lower(month_name_short) as month_name_short,
        month_zero_added
    from {{ source('calendar', 'calendar') }}
) 

select
    last_day((unpivoted.year || '-' || calendar_mapping.month_zero_added || '-01')::date) as calendar_id,
    {{ dbt_utils.generate_surrogate_key(["unpivoted.budget_category"]) }} as category_id,
    unpivoted.transaction_type,
    sum(unpivoted.amount) as amount,
from unpivoted
left join calendar_mapping
    on unpivoted.month = calendar_mapping.month_name_short
group by all