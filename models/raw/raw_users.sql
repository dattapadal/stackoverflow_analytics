with source as (
    select * from {{source('stackoverflow', 'users')  }}
)

select * from source