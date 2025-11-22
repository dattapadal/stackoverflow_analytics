with source as (
    select * from {{ ref('raw_tags')  }}
),
renamed as( 
    select 
        id::bigint as tag_id,
        tagname::varchar as tag_name,
        count::bigint as count,
        excerptpostid::bigint as excerpt_post_id,
        wikipostid::bigint as wiki_post_id
    from source
)

select * from renamed
