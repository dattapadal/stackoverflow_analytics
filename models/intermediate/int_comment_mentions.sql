
with comments as (
    select * from {{ ref('stg_comments') }}
),

questions as (
    select * from {{ ref('int_questions') }}
),

answers as (
    select * from {{ ref('int_answers') }}
),

question_comments as (
    select
        c.comment_id,
        c.post_id,
        'question' as comment_on_type,
        q.question_id,
        q.owner_user_id as question_owner_id,
        c.user_id as commenter_user_id,
        c.comment_text,
        c.creation_date,
        case 
            when c.comment_text like '@%' then true
            else false
        end as has_mention
    from comments c
    inner join questions q
        on c.post_id = q.question_id
),

answer_comments as (
    select
        c.comment_id,
        c.post_id,
        'answer' as comment_on_type,
        a.question_id,
        q.owner_user_id as question_owner_id,
        c.user_id as commenter_user_id,
        c.comment_text,
        c.creation_date,
        case 
            when c.comment_text like '@%' then true
            else false
        end as has_mention
    from comments c
    inner join answers a
        on c.post_id = a.answer_id
    inner join questions q
        on a.question_id = q.question_id
),

all_comments as (
    select * from question_comments
    union all
    select * from answer_comments
),

question_owner_mentions as (
    select *
    from all_comments
    where commenter_user_id = question_owner_id
        and has_mention = true
)

select * from question_owner_mentions