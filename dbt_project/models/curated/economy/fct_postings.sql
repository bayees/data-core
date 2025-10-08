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
        date::date                                                 as calendar_posting_id,
        category_name,
        account_name,
        description,
        amount,
        currency,
        comment,
        counter_account_name
    from postings

        union all

    -- Eliminations
    select 
        date::date                                                 as calendar_posting_id,
        'Anden indkomst'                                           as category_name,
        account_name,
        description,
        -amount,
        currency,
        comment,
        counter_account_name,
    from postings
    where counter_account_name is not null
        and category_name = 'Kontooverførsel'
        and ( 
            ( account_name = 'Lønkonto' and counter_account_name = 'C&V Budget')
            or ( account_name = 'C&V Budget' and counter_account_name = 'Lønkonto' )
        )

)

select 
    {{ dbt_utils.generate_surrogate_key(['category_name']) }},
    * exclude (category_name)
from prepared_postings