{{ 
	config(materialized='external', 
	location='s3://' ~ env_var('ENVIRONMENT', '') ~ '/curated/economy/fct_postings.parquet') 
}}

with
postings as (

    select 
        parent.*,
        child.account_name as counter_account_name 
    from {{ source('spiir', 'transactions') }} as parent
    left join {{ source('spiir', 'transactions') }} as child
        on parent.counter_entry_id = child.id

), 
prepared_postings as (

    select
        date::date                                                              as calendar_posting_id,
        {{ dbt_utils.generate_surrogate_key(['postings.category_name']) }}      as category_id,
        postings.account_name,
        postings.description,
        postings.amount,
        postings.balance,
        postings.currency,
        postings.comment,
        postings.counter_account_name
    from postings

)

select *
from prepared_postings