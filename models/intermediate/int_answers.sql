with posts as (
    select * from {{ ref('stg_posts')  }}
), 
answers as (
    select 
        post_id as answer_id,
        parent_id as question_id,
        owner_user_id,
        score,
        body,
        comment_count,
        creation_date as answer_created_at,
        last_edit_date as answer_last_edit_at
    from posts
    where post_type_id = 2
)
select * from answers