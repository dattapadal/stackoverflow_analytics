with source as (
    select * from {{ ref('raw_votes')  }}
),
renamed as( 
    select 
        id::bigint as vote_id,
        postid::bigint as post_id,
        votetypeid::bigint as vote_type_id,
        creationdate::timestamp as creation_date
    from source
)

select * from renamed
