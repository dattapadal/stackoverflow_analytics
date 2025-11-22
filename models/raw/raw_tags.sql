with source as (
    select * from {{source('stackoverflow', 'tags')  }}
)

select * from source