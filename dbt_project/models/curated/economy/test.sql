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
    where year = 2025 and budget_category = 'institution'
)

select *
from unpivoted
where year = 2025 and budget_category = 'institution'
