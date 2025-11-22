with source as (
    select * from {{ ref('raw_posts')  }}
),
renamed as( 
    select 
        id::bigint as post_id, 
        posttypeid::bigint  as post_type_id,
        acceptedanswerid::bigint as accepted_answer_id,
        parentid::bigint as parent_id,
        owneruserid::bigint as owner_user_id,
        lasteditoruserid::bigint as last_editor_user_id,
        score::bigint as score,
        viewcount::bigint as view_count,
        answercount::bigint as answer_count,
        commentcount::bigint as comment_count,
        favoritecount::bigint as favorite_count,
        body::varchar as body,
        lasteditordisplayname::varchar as last_editor_display_name,
        title::varchar as title,
        tags::varchar as tags,
        contentlicense::varchar as content_license,
        creationdate::timestamp as creation_date,
        lasteditdate::timestamp as last_edit_date,
        lastactivitydate::timestamp as last_activity_date,
        communityowneddate::timestamp as community_owned_date
    from source
)

select * from renamed
