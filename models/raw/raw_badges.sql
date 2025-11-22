with source as (
    select * from {{source('stackoverflow', 'badges')  }}
)

select * from source