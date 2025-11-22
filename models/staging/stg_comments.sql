with source as (
    select * from {{ ref('raw_comments')  }}
),
renamed as( 
    select 
        id::bigint as comment_id,
        postid::bigint as post_id,
        score::bigint as score,
        text::varchar as comment_text,
        userid::bigint as user_id,
        contentlicense::varchar as content_license,
        creationdate::timestamp as creation_date
    from source
)

select * from renamed
