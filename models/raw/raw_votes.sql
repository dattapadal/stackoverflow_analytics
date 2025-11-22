with source as (
    select * from {{source('stackoverflow', 'votes')  }}
)

select * from source