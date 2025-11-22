with source as (
    select * from {{source('stackoverflow', 'comments')  }}
)

select * from source