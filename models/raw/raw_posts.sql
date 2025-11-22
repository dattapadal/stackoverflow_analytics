with source as (
    select * from {{source('stackoverflow', 'posts')  }}
)

select * from source