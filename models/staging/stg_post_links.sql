with source as (
    select * from {{ ref('raw_post_links')  }}
),
renamed as( 
    select 
        id::bigint as post_link_id,
        postid::bigint as post_id,
        relatedpostid::bigint as related_post_id,
        linktypeid::bigint as link_type_id,
        creationdate::timestamp as creation_date
    from source
)

select * from renamed
