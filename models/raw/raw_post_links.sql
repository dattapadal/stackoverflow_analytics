with source as (
    select * from {{source('stackoverflow', 'post_links')  }}
)

select * from source