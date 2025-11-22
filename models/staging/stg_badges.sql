with source as (
    select * from {{ ref('raw_badges')  }}
),
renamed as( 
    select 
        id::bigint as badge_id,
        userid::bigint as user_id,
        name::bigint as name,
        class::bigint as class,
        tagbased::boolean as is_tag_based,
        date::bigint as date,
    from source
)

select * from renamed
