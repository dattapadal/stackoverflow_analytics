with source as (
    select * from {{ ref('raw_users')  }}
),
renamed as( 
    select 
        id::bigint   as user_id,
        reputation::bigint as reputation,
        displayname::varchar  as display_name,
        aboutme::varchar  as about_me,
        views::bigint as views,
        upvotes::bigint as upvotes,
        downvotes::bigint as downvotes,
        creationdate::timestamp as creation_date,
        lastaccessdate::timestamp as last_access_date
    from source
)

select * from renamed
