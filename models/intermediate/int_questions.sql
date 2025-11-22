with posts as (
    select * from {{ ref('stg_posts')  }}
), 
questions as (
    select 
        post_id as question_id,
        owner_user_id,
        accepted_answer_id,
        score,
        view_count,
        answer_count,
        comment_count,
        favorite_count,
        body,
        title,
        tags,
        content_license,
        creation_date as question_created_at,
        last_edit_date as question_last_edited_at,
        last_activity_date as question_last_activity_at
    from posts
    where post_type_id = 1
)
select * from questions